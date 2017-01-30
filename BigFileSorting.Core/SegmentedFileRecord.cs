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
        private string m_Str;

        public ulong Number { get; }
        public byte[] StrAsByteArray { get; private set; }

        public string GetStr(Encoding encoding)
        {
            if (m_Str == null)
            {
                m_Str = encoding.GetString(StrAsByteArray);
            }

            return m_Str;
        }

        public SegmentedFileRecord(ulong number, byte[] strAsByteArray)
        {
            Number = number;
            StrAsByteArray = strAsByteArray;
            m_Str = null;
        }

        public void ClearStr()
        {
            StrAsByteArray = null;
            m_Str = null;
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

            var thisStr = GetStr(encoding);
            var otherStr = other.GetStr(encoding);

            return String.CompareOrdinal(thisStr, otherStr);
        }
    }
}
