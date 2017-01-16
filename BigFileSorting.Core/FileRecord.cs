using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigFileSorting.Core
{
    internal struct FileRecord<TNumber>
    {
        public TNumber Number { get; }
        public byte[] Str { get; }

        public FileRecord(TNumber number, byte[] str)
        {
            Number = number;
            Str = str;
        }
    }
}
