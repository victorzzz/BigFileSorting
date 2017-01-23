using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace BigFileSorting.Core
{
    internal struct SegmentedFileRecord
    {
        public ulong Number { get; }
        public byte[] StrAsByteArray { get; private set; }

        private string Str;

        public SegmentedFileRecord(ulong number, byte[] strAsByteArray)
        {
            Number = number;
            StrAsByteArray = strAsByteArray;
            Str = null;
        }

        public int CompareTo(SegmentedFileRecord other, Encoding encoding)
        {
            if (Number < other.Number)
            {
                return -1;
            }

            if (Number > other.Number)
            {
                return 1;
            }

            if (Str == null)
            {
                Str = encoding.GetString(StrAsByteArray);
            }

            if (other.Str == null)
            {
                other.Str = encoding.GetString(other.StrAsByteArray);
            }

            return String.CompareOrdinal(Str, other.Str);
        }
    }
}
