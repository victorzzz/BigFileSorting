using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using System.Diagnostics;
using BigFileSorting.Core;

namespace BigFileSorting.Core.Utils
{
    internal static class Memory
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
        public static long GetAvaliablePhisicalMemory()
        {
            var pi = new PerformanceInformation();

            long avaliableMemory;

            if (GetPerformanceInfo(out pi, Marshal.SizeOf(pi)))
            {
                avaliableMemory = Convert.ToInt64(pi.PhysicalAvailable.ToInt64() * pi.PageSize.ToInt64());
            }
            else
            {
                avaliableMemory = Constants.DEFAULT_AVALIABLE_MEMORY;
            }

            return avaliableMemory;
        }
    }
}
