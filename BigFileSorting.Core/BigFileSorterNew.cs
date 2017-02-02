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
            Trace.WriteLine("START!");

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

            // check temp dirs
            tempDirs = TempDirectoryHelper.TempDirsToUse(tempDirs, m_CancellationToken);

            if (memoryToUse <= 0)
            {
                memoryToUse = Memory.GetAvaliablePhisicalMemory();
            }

            Trace.WriteLine($"memoryToUse:{memoryToUse}");

            var listStructSize = Marshal.SizeOf(typeof(FileRecord));

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

                        var memoryForData = memoryToUse / 4 * 3;

                        var listCapasityLimit = Math.Min(
                            (int)(memoryForData / Constants.APROXIMATE_RECORD_SIZE),
                            Constants.MAX_ARRAY_BYTES / listStructSize);

                        var segmentSizeLimit = memoryForData - listCapasityLimit * listStructSize;
                        var currentSegmentSize = 0;

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

                        listToSort.Sort();

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
                                    fileWriter.WriteOriginalSegment(listToSort);
                                }
                                else
                                {
                                    ((IDisposable)targetTempFile).Dispose();
                                    MergeAndWrite(sourceTempFile, fileWriter, listToSort);
                                }

                                fileWriter.FlushDataAndDisposeFiles();

                                break;
                            }
                        }
                        else
                        {
                            targetTempFile.SwitchToNewFile();

                            if (firstSegment)
                            {
                                var targetTempFileLocal = targetTempFile;
                                targetTempFileLocal.WriteSortedSegment(listToSort);
                            }
                            else
                            {
                                var sourceTempFileLocal = sourceTempFile;
                                var targetTempFileLocal = targetTempFile;
                                MergeAndWrite(sourceTempFileLocal, targetTempFileLocal, listToSort);
                            }
                        }

                        firstSegment = false;

                        Trace.WriteLine("Swapping source and destination temp files");
                        var t = sourceTempFile;
                        sourceTempFile = targetTempFile;
                        targetTempFile = t;
                    }
                }
            }
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
