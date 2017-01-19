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

        public static async Task<Tuple<long, long>> Sort(
            string sourceFilePath,
            string targetDir,
            IReadOnlyCollection<string> tempDirs,
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

            Stopwatch read_sw = new Stopwatch();
            Stopwatch sort_sw = new Stopwatch();

            //read source file, devide into segments, sort segments in the memory, put segments into temp files on temp directories
            using (var fileReader = new BigFileReader(sourceFilePath, 1024 * 1024 * 4, avaliableMemory / 2, encoding, cancellationToken))
            {
                while (true)
                {
                    read_sw.Start();
                    var segment = await fileReader.ReadSegment();
                    if(segment.Count == 0)
                    {
                        break;
                    }
                    read_sw.Stop();

                    sort_sw.Start();
                    /*
                    segment.Sort((FileRecord r1, FileRecord r2) =>
                    {
                        if (r1.Number < r2.Number)
                        {
                            return -1;
                        }

                        if (r1.Number > r2.Number)
                        {
                            return 1;
                        }

                        return string.Compare(r1.Str, r2.Str, StringComparison.Ordinal);
                    });
                    */

                    segment.Sort();

                    sort_sw.Stop();
                }
            }

            return new Tuple<long, long>(read_sw.ElapsedMilliseconds, sort_sw.ElapsedMilliseconds);
        }
    }
}
