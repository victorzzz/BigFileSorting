using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace BigFileSorting.Core
{

    public struct TempFileRecord
    {
        public string Str { get; private set; }

        public ulong Number { get; }
       

        public TempFileRecord(ulong number, byte[] strAsByteArray, Encoding encoding)
        {
            Number = number;
            Str = encoding.GetString(strAsByteArray);
        }

        public void ClearStr()
        {
            Str = null;
        }

        public int CompareTo(TempFileRecord other)
        {
            var strSomparisionResult = String.CompareOrdinal(Str, other.Str);

            if (strSomparisionResult == 0)
            {
                return Number.CompareTo(other.Number);
            }
            else
            {
                return strSomparisionResult;
            }
        }
    }
}
