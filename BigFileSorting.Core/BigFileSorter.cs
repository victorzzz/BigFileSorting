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
    public static class BigFileSorter
    {

        /// <summary>
        /// 
        /// NOT thread safe!
        /// </summary>
        /// <param name="sourceFilePath"></param>
        /// <param name="targetDir"></param>
        /// <param name="tempDirs"></param>
        /// <param name="encoding"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task<Tuple<long, long, long>> Sort(
            string sourceFilePath,
            string targetDir,
            IReadOnlyList<string> tempDirs,
            Encoding encoding,
            CancellationToken cancellationToken)
        {          
            if (string.IsNullOrWhiteSpace(sourceFilePath))
            {
                throw new ArgumentNullException(nameof(sourceFilePath));
            }

            if (string.IsNullOrWhiteSpace(targetDir))
            {
                throw new ArgumentNullException(nameof(targetDir));
            }

            if (tempDirs == null)
            {
                throw new ArgumentNullException(nameof(tempDirs));
            }

            // check temp dirs
            tempDirs = TempDirectoryHelper.TempDirsToUse(tempDirs, cancellationToken);
            var memoryToUse = Memory.GetAvaliablePhisicalMemory() * 2 / 3;

            var fibonachiSegmentsDistribution = new FibonachiSegmentsDistribution();

            Stopwatch read_sw = new Stopwatch();
            Stopwatch sort_sw = new Stopwatch();
            Stopwatch write_sw = new Stopwatch();

            List<TempSegmentedFile> tempFiles = new List<TempSegmentedFile>(capacity: 3);

            using (var tempFile0 = new TempSegmentedFile(tempDirs[0], encoding, cancellationToken))
            using (var tempFile1 = new TempSegmentedFile(tempDirs[1], encoding, cancellationToken))
            using (var tempFile2 = new TempSegmentedFile(tempDirs[2], encoding, cancellationToken))
            {
                tempFiles.Add(tempFile0);
                tempFiles.Add(tempFile1);
                tempFiles.Add(tempFile2);

                await Task.WhenAll(
                    tempFile0.SwitchToNewFileAsync(),
                    tempFile1.SwitchToNewFileAsync()
                    ).ConfigureAwait(false);

                int numberOfSegment = 1;
                //read source file, devide into segments, sort segments in the memory, put segments into temp files on temp directories
                using (var fileReader = new BigFileReader(sourceFilePath, memoryToUse, encoding, cancellationToken))
                {
                    while (true)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        read_sw.Start();
                        var segment = await fileReader.ReadSegmentAsync().ConfigureAwait(false);
                        if (segment.Count == 0)
                        {
                            break;
                        }
                        read_sw.Stop();

                        sort_sw.Start();
                        segment.Sort();
                        sort_sw.Stop();

                        if (fileReader.EndOfFile())
                        {

                            break;
                        }

                        write_sw.Start();
                        
                        // distribute segments into two temp files
                        var tempFileToWriteSegment = tempFiles[fibonachiSegmentsDistribution.NextTempFileIndex(numberOfSegment)];
                        await tempFileToWriteSegment.WriteSortedSegmentAsync(segment).ConfigureAwait(false);

                        write_sw.Stop();
                    }
                }

                var sourceFile0 = tempFile0;
                var sourceFile1 = tempFile1;
                var targetFile = tempFile2;

                // merging, merging, merging ...
                while (true)
                {
                    if(sourceFile0.NumberOfSegments == 1 && sourceFile1.NumberOfSegments == 1)
                    {
                        break;
                    }

                    await SegmentedFileMerger.Merge(
                        new List<TempSegmentedFile>{ sourceFile0, sourceFile1},
                        targetFile, 
                        encoding, 
                        cancellationToken).ConfigureAwait(false);

                    var t = targetFile;

                    // find fully read temp file
                    if (sourceFile0.NumberOfSegments == 0)
                    {
                        targetFile = sourceFile0;
                        sourceFile0 = t;
                    }
                    else
                    {
                        targetFile = sourceFile1;
                        sourceFile1 = t;
                    }

                    await Task.WhenAll(
                        targetFile.SwitchToNewFileAsync(),
                        t.SwitchToReadModeAsync()).ConfigureAwait(false);
                }
            }

            return new Tuple<long, long, long>(
                read_sw.ElapsedMilliseconds,
                sort_sw.ElapsedMilliseconds,
                write_sw.ElapsedMilliseconds);
        }
    }
}
