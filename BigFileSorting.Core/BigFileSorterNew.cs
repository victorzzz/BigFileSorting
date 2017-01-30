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
    public static class BigFileSorterNew
    {
        /// <summary>
        /// 
        /// NOT thread safe!
        /// </summary>
        /// <param name="sourceFilePath"></param>
        /// <param name="targetFilePath"></param>
        /// <param name="tempDirs"></param>
        /// <param name="encoding"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task Sort(
            string sourceFilePath,
            string targetFilePath,
            IReadOnlyList<string> tempDirs,
            Encoding encoding,
            CancellationToken cancellationToken)
        {
            Console.WriteLine("START!");

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
            tempDirs = TempDirectoryHelper.TempDirsToUse(tempDirs, cancellationToken);
            var memoryToUse = Memory.GetAvaliablePhisicalMemory() * 1 / 15;

            Console.WriteLine("memoryToUse:{0}", memoryToUse);

            using (var tempFile0 = new TempFile(tempDirs[0], encoding, cancellationToken))
            using (var tempFile1 = new TempFile(tempDirs[1], encoding, cancellationToken))
            {
                var sourceTempFile = tempFile0;
                var targetTempFile = tempFile1;

                using (var fileReader = new BigFileReader(sourceFilePath, memoryToUse, encoding, cancellationToken))
                {
                    bool firstSegment = true;

                    while (true)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        var segment = await fileReader.ReadSegmentAsync().ConfigureAwait(false);
                        if (segment.Count == 0)
                        {
                            break;
                        }

                        Console.WriteLine("start sorting:{0}", segment.Count);

                        segment.Sort();

                        Console.WriteLine("Sorted!");

                        if (fileReader.EndOfFile())
                        {
                            using (var fileWriter = new BigFileWriter(targetFilePath, encoding, cancellationToken))
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
                                    await MergeAndWriteAsync(sourceTempFile, fileWriter, segment, encoding, cancellationToken).ConfigureAwait(false);
                                }

                                await fileWriter.FlushDataAndDisposeFilesAsync().ConfigureAwait(false);
                                break;
                            }
                        }
                        else
                        {
                            await targetTempFile.SwitchToNewFileAsync().ConfigureAwait(false);
                            if (firstSegment)
                            {
                                await targetTempFile.WriteSortedSegmentAsync(segment);
                            }
                            else
                            {
                                await MergeAndWriteAsync(sourceTempFile, targetTempFile, segment, encoding, cancellationToken).ConfigureAwait(false);
                            }
                        }

                        firstSegment = false;

                        var t = sourceTempFile;
                        sourceTempFile = targetTempFile;
                        targetTempFile = t;
                    }
                }
            }
        }

        private static async Task MergeAndWriteAsync(TempFile sourceTempFile,
            IFileWritter destinationFile,
            IReadOnlyList<FileRecord> lastSegment,
            Encoding encoding,
            CancellationToken cancellationToken)
        {
            await sourceTempFile.SwitchToReadModeAsync().ConfigureAwait(false);

            var tempFileRecord = await sourceTempFile.ReadRecordToMergeAsync().ConfigureAwait(false);

            foreach (var lastSegmentRecord in lastSegment)
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (!tempFileRecord.HasValue)
                    {
                        await destinationFile.WriteOriginalFileRecordAsync(lastSegmentRecord).ConfigureAwait(false);
                        break;
                    }
                    else
                    {
                        if (lastSegmentRecord.CompareTo(tempFileRecord.Value, encoding) < 0)
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
                cancellationToken.ThrowIfCancellationRequested();

                await destinationFile.WriteSegmentedFileRecordAsync(tempFileRecord.Value).ConfigureAwait(false);
                tempFileRecord = await sourceTempFile.ReadRecordToMergeAsync().ConfigureAwait(false);
            }
        }
    }
}
