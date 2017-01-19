using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace BigFileSorting.Core
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal struct FileRecord : IComparable<FileRecord>, IEquatable<FileRecord>
    {
        public long Number { get; }
        public string Str { get; }

        public FileRecord(long number, string str)
        {
            Number = number;
            Str = str;
        }

        int IComparable<FileRecord>.CompareTo(FileRecord other)
        {
            if (Number < other.Number)
            {
                return -1;
            }

            if (Number > other.Number)
            {
                return 1;
            }

            return string.Compare(Str, other.Str, StringComparison.Ordinal);
        }

        bool IEquatable<FileRecord>.Equals(FileRecord other)
        {
            return Number == other.Number && Str.Equals(other.Str, StringComparison.Ordinal);
        }
    }
}
