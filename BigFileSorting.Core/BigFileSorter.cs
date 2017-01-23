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

namespace BigFileSorting.Core
{
    public static class BigFileSorter
    {
        [StructLayout(LayoutKind.Sequential)]
        public struct PerformanceInformation
        {
            public int Size;
            public IntPtr CommitTotal;
            public IntPtr CommitLimit;
            public IntPtr CommitPeak;
            public IntPtr PhysicalTotal;
            public IntPtr PhysicalAvailable;
            public IntPtr SystemCache;
            public IntPtr KernelTotal;
            public IntPtr KernelPaged;
            public IntPtr KernelNonPaged;
            public IntPtr PageSize;
            public int HandlesCount;
            public int ProcessCount;
            public int ThreadCount;
        }

        [DllImport("psapi.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool GetPerformanceInfo(
            [Out] out PerformanceInformation PerformanceInformation,
            [In] int Size);

        private static int[] m_Fibonachi = { 1, 1 };
        private static int[] m_ActualSegments = { 0, 0 };
        private static int m_TempFileIndexToWriteSegment = 0;

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
        public static async Task<Tuple<long, long>> Sort(
            string sourceFilePath,
            string targetDir,
            IReadOnlyList<string> tempDirs,
            Encoding encoding,
            CancellationToken cancellationToken)
        {
            m_Fibonachi = new int[] { 1, 1 };
            m_ActualSegments = new int[] { 0, 0 };

            m_LastTempFileIndexToWriteSegment = 0;

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
            var memoryToUse = GetAvaliablePhisicalMemory() * 2 / 3;

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

                int numberOfSegment = 1;
                //read source file, devide into segments, sort segments in the memory, put segments into temp files on temp directories
                using (var fileReader = new BigFileReader(sourceFilePath, memoryToUse, encoding, cancellationToken))
                {
                    while (true)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        read_sw.Start();
                        var segment = await fileReader.ReadSegment().ConfigureAwait(false);
                        if (segment.Count == 0)
                        {
                            break;
                        }
                        read_sw.Stop();

                        sort_sw.Start();
                        segment.Sort();
                        sort_sw.Stop();

                        write_sw.Start();
                        
                        // distribute segments into two temp files
                        var tempFileToWriteSegment = tempFiles[NextTempFileIndex(numberOfSegment)];
                        await tempFileToWriteSegment.WriteSortedSegmentAsync(segment).ConfigureAwait(false);

                        write_sw.Stop();
                    }
                }
            }
            return new Tuple<long, long, long>(
                read_sw.ElapsedMilliseconds,
                sort_sw.ElapsedMilliseconds,
                write_sw.ElapsedMilliseconds);
        }

        private static int NextTempFileIndex(int numberOfSegment)
        {
            if (numberOfSegment != m_ActualSegments.Sum() + 1)
            {
                throw new InvalidCastException();
            }

            while (true)
            {
                int result;
                if (m_ActualSegments[m_TempFileIndexToWriteSegment] < m_Fibonachi[m_TempFileIndexToWriteSegment])
                {
                    ++m_ActualSegments[m_TempFileIndexToWriteSegment];
                    result = m_TempFileIndexToWriteSegment;
                    m_TempFileIndexToWriteSegment = 1 - m_TempFileIndexToWriteSegment;

                    return result;
                }

                int newFibonachi = m_Fibonachi.Sum();
                m_Fibonachi[0] = m_Fibonachi[1];
                m_Fibonachi[1] = newFibonachi;
            }
        }

        private static long GetAvaliablePhisicalMemory()
        {
            var pi = new PerformanceInformation();

            long avaliableMemory;

            if (GetPerformanceInfo(out pi, Marshal.SizeOf(pi)))
            {
                avaliableMemory = Convert.ToInt64(pi.PhysicalAvailable.ToInt64() * pi.PageSize.ToInt64());
            }
            else
            {
                avaliableMemory = 1024L * 1024L * 1024L * 4L; // 4Gb
            }

            return avaliableMemory;
        }
    }
}
