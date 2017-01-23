using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

using BigFileSorting.Core.Utils;

namespace BigFileSorting.Core
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal struct FileRecord : IComparable<FileRecord>
    {
        public ulong Number { get; }
        public string Str { get; }

        public FileRecord(ulong number, string str)
        {
            Number = number;
            Str = str;
        }

        public int CompareTo(FileRecord other)
        {
            if (Number < other.Number)
            {
                return -1;
            }

            if (Number > other.Number)
            {
                return 1;
            }

            return String.CompareOrdinal(Str, other.Str);
        }
    }
}
