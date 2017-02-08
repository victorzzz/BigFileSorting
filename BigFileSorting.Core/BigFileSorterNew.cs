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
        /// <param name="memoryToUse"></param>
        /// <returns></returns>
        public void Sort(
            string sourceFilePath,
            string targetFilePath,
            IReadOnlyList<string> tempDirs,
            long memoryToUse)
        {
            var sw_total = Stopwatch.StartNew();

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

            if (memoryToUse <= 0)
            {
                memoryToUse = Memory.GetAvaliablePhisicalMemory();
            }

            long memoryForData = (long)(memoryToUse * Constants.PART_OF_MEMORY_TO_USE);
            int listStructSize = Marshal.SizeOf(typeof(FileRecord));

            int listCapasityLimit = Math.Min(
                (int)(memoryForData / Constants.APROXIMATE_RECORD_SIZE),
                Constants.MAX_ARRAY_BYTES / listStructSize);

            long segmentSizeLimit = memoryForData - listCapasityLimit * listStructSize;

            Console.WriteLine($"memoryToUse:{memoryToUse}; total GC memory:{GC.GetTotalMemory(true)}; segment size limit:{segmentSizeLimit}");

            using (var tempFile0 = new TempFile(tempDirs[0], m_Encoding, m_CancellationToken))
            using (var tempFile1 = new TempFile(tempDirs[1], m_Encoding, m_CancellationToken))
            {
                var sourceTempFile = tempFile0;
                var targetTempFile = tempFile1;

                using (var fileReader = new BigFileReader(sourceFilePath, m_Encoding, m_CancellationToken))
                {
                    bool firstSegment = true;

                    while (true)
                    {
                        m_CancellationToken.ThrowIfCancellationRequested();

                        GC.Collect(GC.MaxGeneration, GCCollectionMode.Optimized, false);

                        long currentSegmentSize = 0L;

                        Console.WriteLine("Reading new segment to sort.");
                        var listToSort = new List<FileRecord>(capacity: listCapasityLimit);

                        while(true)
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
                            Console.WriteLine($"currentSegmentSize:{currentSegmentSize}; total GC Memory:{totalGCMemory}; phisical memory:{phisicalMemory}");
                        }

                        Console.WriteLine("Start sorting");
                        var sw_sort = Stopwatch.StartNew();
                        listToSort.Sort();
                        sw_sort.Stop();
                        Console.WriteLine($"Segment sorting time: {sw_sort.Elapsed}");

                        m_CancellationToken.ThrowIfCancellationRequested();

                        if (fileReader.EndOfFile())
                        {
                            using (var fileWriter = new BigFileWriter(targetFilePath, m_Encoding, m_CancellationToken))
                            {
                                ((IDisposable)fileReader).Dispose();

                                if (firstSegment)
                                {
                                    ((IDisposable)sourceTempFile).Dispose();
                                    ((IDisposable)targetTempFile).Dispose();

                                    // the whole file is already sorted
                                    // we don't need to use any temporary files
                                    // just write it to the destination file

                                    Console.WriteLine("Final writing of sorted segment ... !");
                                    fileWriter.WriteOriginalSegment(listToSort);
                                }
                                else
                                {
                                    ((IDisposable)targetTempFile).Dispose();

                                    Console.WriteLine("Final merging ... !");
                                    MergeAndWrite(sourceTempFile, fileWriter, listToSort);
                                }

                                fileWriter.FlushDataAndDisposeFiles();

                                break;
                            }
                        }
                        else
                        {
                            Console.WriteLine("Switching to new temp file.");
                            targetTempFile.SwitchToNewFile();

                            if (firstSegment)
                            {
                                Console.WriteLine("Writing of sorted segment to temp file.");
                                targetTempFile.WriteOriginalSegment(listToSort);
                            }
                            else
                            {
                                Console.WriteLine("Merging to temp file.");
                                MergeAndWrite(sourceTempFile, targetTempFile, listToSort);
                            }
                        }

                        firstSegment = false;

                        Console.WriteLine("Swapping source and destination temp files");
                        var t = sourceTempFile;
                        sourceTempFile = targetTempFile;
                        targetTempFile = t;
                    }
                }
            }

            sw_total.Stop();
            Console.WriteLine($"Total time: {sw_total.Elapsed}");
        }

        private void MergeAndWrite(TempFile sourceTempFile,
            IFileWritter destinationFile,
            IReadOnlyList<FileRecord> lastSegment)
        {
            sourceTempFile.SwitchToReadMode();

            var tempFileRecord = sourceTempFile.ReadRecordToMerge();

            foreach (var lastSegmentRecord in lastSegment)
            {
                while (true)
                {
                    m_CancellationToken.ThrowIfCancellationRequested();

                    if (!tempFileRecord.HasValue)
                    {
                        destinationFile.WriteOriginalFileRecord(lastSegmentRecord);
                        break;
                    }
                    else
                    {
                        if (lastSegmentRecord.CompareTo(tempFileRecord.Value, m_Encoding) < 0)
                        {
                            destinationFile.WriteOriginalFileRecord(lastSegmentRecord);
                            break;
                        }
                        else
                        {
                            destinationFile.WriteTempFileRecord(tempFileRecord.Value);
                            tempFileRecord = sourceTempFile.ReadRecordToMerge();
                        }
                    }
                }
            }

            while(tempFileRecord.HasValue)
            {
                m_CancellationToken.ThrowIfCancellationRequested();

                destinationFile.WriteTempFileRecord(tempFileRecord.Value);
                tempFileRecord = sourceTempFile.ReadRecordToMerge();
            }
        }
    }
}
