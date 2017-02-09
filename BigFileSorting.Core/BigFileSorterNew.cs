using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Diagnostics;
using BigFileSorting.Core.Utils;

namespace BigFileSorting.Core
{
    public class BigFileSorterNew
    {
        private readonly CancellationToken m_CancellationToken;
        private readonly Encoding m_Encoding;

        public BigFileSorterNew(CancellationToken cancellationToken, Encoding encoding)
        {
            m_CancellationToken = cancellationToken;
            m_Encoding = encoding;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sourceFilePath"></param>
        /// <param name="targetFilePath"></param>
        /// <param name="tempDirs"></param>
        /// <param name="incomingMemoryToUse"></param>
        /// <returns></returns>
        public void Sort(
            string sourceFilePath,
            string targetFilePath,
            IReadOnlyList<string> tempDirs,
            long incomingMemoryToUse)
        {
            var swTotal = Stopwatch.StartNew();

            if (string.IsNullOrWhiteSpace(sourceFilePath))
            {
                throw new ArgumentNullException(nameof(sourceFilePath));
            }

            if (string.IsNullOrWhiteSpace(targetFilePath))
            {
                throw new ArgumentNullException(nameof(targetFilePath));
            }

            if (tempDirs == null)
            {
                throw new ArgumentNullException(nameof(tempDirs));
            }

            tempDirs = TempDirectoryHelper.TempDirsToUse(tempDirs);

            using (var tempFile0 = new TempFile(tempDirs[0], m_Encoding, m_CancellationToken))
            using (var tempFile1 = new TempFile(tempDirs[1], m_Encoding, m_CancellationToken))
            {
                var sourceTempFile = tempFile0;
                var targetTempFile = tempFile1;

                using (var fileReader = new BigFileReader(sourceFilePath, m_Encoding, m_CancellationToken))
                {
                    bool isFirstSegment = true;

                    while (true)
                    {
                        m_CancellationToken.ThrowIfCancellationRequested();

                        var listToSort = ReadNextSegment(fileReader, incomingMemoryToUse);

                        System.Console.WriteLine("Start sorting");
                        var swSort = Stopwatch.StartNew();
                        listToSort.Sort();
                        swSort.Stop();
                        System.Console.WriteLine($"Segment sorting time: {swSort.Elapsed}");

                        m_CancellationToken.ThrowIfCancellationRequested();

                        if (fileReader.EndOfFile())
                        {
                            System.Console.WriteLine("Writing to target file!");

                            ((IDisposable)fileReader).Dispose();

                            WriteLastSegment(listToSort, targetFilePath, isFirstSegment, sourceTempFile, targetTempFile);

                            break;
                        }
                        else
                        {
                            System.Console.WriteLine("Switching to new temp file.");
                            targetTempFile.SwitchToNewFile();

                            if (isFirstSegment)
                            {
                                System.Console.WriteLine("Writing of sorted segment to temp file.");
                                targetTempFile.WriteOriginalSegment(listToSort);
                            }
                            else
                            {
                                System.Console.WriteLine("Merging to temp file.");
                                MergeAndWrite(sourceTempFile, targetTempFile, listToSort);
                            }
                        }

                        isFirstSegment = false;

                        System.Console.WriteLine("Swapping source and destination temp files");
                        var t = sourceTempFile;
                        sourceTempFile = targetTempFile;
                        targetTempFile = t;
                    }
                }
            }

            swTotal.Stop();
            System.Console.WriteLine($"Total time: {swTotal.Elapsed}");
        }

        private void WriteLastSegment(List<FileRecord> segment,
            string targetFilePath,
            bool firstSegment,
            TempFile sourceTempFile,
            TempFile targetTempFile)
        {
            using (var fileWriter = new BigFileWriter(targetFilePath, m_Encoding, m_CancellationToken))
            {
                if (firstSegment)
                {
                    ((IDisposable)sourceTempFile).Dispose();
                    ((IDisposable)targetTempFile).Dispose();

                    // the whole file is already sorted
                    // we don't need to use any temporary files
                    // just write it to the destination file

                    System.Console.WriteLine("Final writing of sorted segment ... !");
                    fileWriter.WriteOriginalSegment(segment);
                }
                else
                {
                    ((IDisposable)targetTempFile).Dispose();

                    System.Console.WriteLine("Final merging ... !");
                    MergeAndWrite(sourceTempFile, fileWriter, segment);
                }

                fileWriter.FlushDataAndDisposeFiles();
            }
        }

        private List<FileRecord> ReadNextSegment(BigFileReader fileReader, long incomingMemoryToUse)
        {
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true);

            long gcTotalMemory = GC.GetTotalMemory(true);

            long memoryToUse;
            if (incomingMemoryToUse == 0)
            {
                memoryToUse = Memory.GetAvaliablePhisicalMemory();
            }
            else
            {
                memoryToUse = incomingMemoryToUse;
            }

            long memoryForData = (long)(memoryToUse * Constants.PART_OF_MEMORY_TO_USE);
            int listStructSize = Marshal.SizeOf(typeof(FileRecord));
            int listCapasityLimit = Math.Min(
                (int)(memoryForData / Constants.APROXIMATE_RECORD_SIZE),
                Constants.MAX_ARRAY_BYTES / listStructSize);
            long segmentSizeLimit = memoryForData - listCapasityLimit * listStructSize;
            System.Console.WriteLine($"Before reading next segment. Phisical memory To Use:{memoryToUse}; total GC memory:{gcTotalMemory}; segment size limit:{segmentSizeLimit}");

            long currentSegmentSize = 0L;
            var listToSort = new List<FileRecord>(capacity: listCapasityLimit);

            while (true)
            {
                m_CancellationToken.ThrowIfCancellationRequested();

                var record = fileReader.ReadRecord();
                if (!record.HasValue)
                {
                    break;
                }

                listToSort.Add(record.Value);
                currentSegmentSize += record.Value.Str.Length * 2;

                if (currentSegmentSize > segmentSizeLimit || listToSort.Count >= listCapasityLimit)
                {
                    break;
                }
            }

            {
                long totalGCMemory = GC.GetTotalMemory(true);
                long phisicalMemory = Memory.GetAvaliablePhisicalMemory();
                System.Console.WriteLine($"Next segment was read!. CurrentSegmentSize:{currentSegmentSize}; total GC Memory:{totalGCMemory}; physical memory:{phisicalMemory}");
            }

            return listToSort;
        }

        private void MergeAndWrite(TempFile sourceTempFile,
            IFileWritter destinationFile,
            IReadOnlyList<FileRecord> lastSegment)
        {
            sourceTempFile.SwitchToReadMode();

            var fileRecord = sourceTempFile.ReadRecordToMerge();

            foreach (var lastSegmentRecord in lastSegment)
            {
                while (true)
                {
                    m_CancellationToken.ThrowIfCancellationRequested();

                    if (!fileRecord.HasValue)
                    {
                        destinationFile.WriteFileRecord(lastSegmentRecord);
                        break;
                    }
                    else
                    {
                        if (lastSegmentRecord.CompareTo(fileRecord.Value) < 0)
                        {
                            destinationFile.WriteFileRecord(lastSegmentRecord);
                            break;
                        }
                        else
                        {
                            destinationFile.WriteFileRecord(fileRecord.Value);
                            fileRecord = sourceTempFile.ReadRecordToMerge();
                        }
                    }
                }
            }

            while(fileRecord.HasValue)
            {
                m_CancellationToken.ThrowIfCancellationRequested();

                destinationFile.WriteFileRecord(fileRecord.Value);
                fileRecord = sourceTempFile.ReadRecordToMerge();
            }
        }
    }
}
