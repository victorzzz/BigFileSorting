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
    public struct FileRecord : IComparable<FileRecord>
    {
        public ulong Number { get; }
        public string Str { get; private set; }

        public FileRecord(ulong number, string str)
        {
            Number = number;
            Str = str;
        }

        public FileRecord(ulong number, byte[] strAsByteArray, Encoding encoding)
        {
            Number = number;
            Str = encoding.GetString(strAsByteArray);
        }

        public void ClearStr()
        {
            Str = null;
        }

        public int CompareTo(FileRecord other)
        {
            var strComparisionResult = String.CompareOrdinal(Str, other.Str);

            if (strComparisionResult == 0)
            {
                return Number.CompareTo(other.Number);
            }
            else
            {
                return strComparisionResult;
            }
        }
    }
}
