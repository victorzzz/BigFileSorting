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
        private readonly ProactiveTaskRunner m_ProactiveTaskRunner;
        private readonly CancellationToken m_CancellationToken;
        private readonly Encoding m_Encoding;

        public BigFileSorterNew(CancellationToken cancellationToken, Encoding encoding)
        {
            m_ProactiveTaskRunner = new ProactiveTaskRunner(cancellationToken);
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
        public async Task Sort(
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
                memoryToUse = Memory.GetAvaliablePhisicalMemory() * 3 / 4;
            }

            Trace.WriteLine($"memoryToUse:{memoryToUse}");

            var limiter = new TotalAllocatedMemoryLimiter(false, m_CancellationToken);

            using (var tempFile0 = new TempFile(tempDirs[0], m_Encoding, limiter, m_CancellationToken))
            using (var tempFile1 = new TempFile(tempDirs[1], m_Encoding, limiter, m_CancellationToken))
            {
                var sourceTempFile = tempFile0;
                var targetTempFile = tempFile1;

                using (var fileReader = new BigFileReader(sourceFilePath, memoryToUse, m_Encoding, limiter, m_CancellationToken))
                {
                    bool firstSegment = true;

                    while (true)
                    {
                        m_CancellationToken.ThrowIfCancellationRequested();

                        var segment = await fileReader.ReadSegmentAsync().ConfigureAwait(false);
                        if (segment.Count == 0)
                        {
                            break;
                        }

                        Trace.WriteLine($"start sorting:{segment.Count}");

                        var sw_sorting = Stopwatch.StartNew();

                        segment.Sort();

                        sw_sorting.Stop();

                        Trace.WriteLine($"Sorted! {sw_sorting.Elapsed}");

                        await m_ProactiveTaskRunner.WaitForProactiveTaskAsync().ConfigureAwait(false);

                        if (fileReader.EndOfFile())
                        {
                            using (var fileWriter = new BigFileWriter(targetFilePath, m_Encoding, m_CancellationToken))
                            {
                                if (firstSegment)
                                {
                                    // the whole file is already sorted
                                    // we don't need to use any temporary files
                                    // just write it to the destination file
                                    await fileWriter.WriteOriginalSegmentAsync(segment).ConfigureAwait(false);
                                }
                                else
                                {
                                    await MergeAndWriteAsync(sourceTempFile, fileWriter, segment, limiter).ConfigureAwait(false);
                                }

                                await fileWriter.FlushDataAndDisposeFilesAsync().ConfigureAwait(false);

                                break;
                            }
                        }
                        else
                        {
                            await targetTempFile.SwitchToNewFileAsync().ConfigureAwait(false);

                            await limiter.ResetLimit();

                            if (firstSegment)
                            {
                                var targetTempFileLocal = targetTempFile;
                                m_ProactiveTaskRunner.StartProactiveTask(
                                    async () => await targetTempFileLocal.WriteSortedSegmentAsync(segment).ConfigureAwait(false));
                            }
                            else
                            {
                                var sourceTempFileLocal = sourceTempFile;
                                var targetTempFileLocal = targetTempFile;
                                m_ProactiveTaskRunner.StartProactiveTask(
                                    async () => await MergeAndWriteAsync(sourceTempFileLocal, targetTempFileLocal, segment, limiter).ConfigureAwait(false));
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

        private async Task MergeAndWriteAsync(TempFile sourceTempFile,
            IFileWritter destinationFile,
            IReadOnlyList<FileRecord> lastSegment,
            TotalAllocatedMemoryLimiter limiter)
        {
            await sourceTempFile.SwitchToReadModeAsync().ConfigureAwait(false);

            var tempFileRecord = await sourceTempFile.ReadRecordToMergeAsync().ConfigureAwait(false);

            foreach (var lastSegmentRecord in lastSegment)
            {
                while (true)
                {
                    m_CancellationToken.ThrowIfCancellationRequested();

                    if (!tempFileRecord.HasValue)
                    {
                        await destinationFile.WriteOriginalFileRecordAsync(lastSegmentRecord).ConfigureAwait(false);
                        break;
                    }
                    else
                    {
                        if (lastSegmentRecord.CompareTo(tempFileRecord.Value, m_Encoding) < 0)
                        {
                            await destinationFile.WriteOriginalFileRecordAsync(lastSegmentRecord).ConfigureAwait(false);
                            break;
                        }
                        else
                        {
                            await destinationFile.WriteSegmentedFileRecordAsync(tempFileRecord.Value).ConfigureAwait(false);
                            tempFileRecord = await sourceTempFile.ReadRecordToMergeAsync().ConfigureAwait(false);
                        }
                    }
                }
            }

            while(tempFileRecord.HasValue)
            {
                m_CancellationToken.ThrowIfCancellationRequested();

                await destinationFile.WriteSegmentedFileRecordAsync(tempFileRecord.Value).ConfigureAwait(false);
                tempFileRecord = await sourceTempFile.ReadRecordToMergeAsync().ConfigureAwait(false);
            }

            limiter.TurnOff();
        }
    }
}
