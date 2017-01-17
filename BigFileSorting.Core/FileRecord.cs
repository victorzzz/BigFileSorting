using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigFileSorting.Core
{
    internal class FileRecord<TNumber> : IComparable<FileRecord<TNumber>>, IEquatable<FileRecord<TNumber>> 
        where TNumber : struct, IComparable<TNumber>, IEquatable<TNumber>
    {
        public TNumber Number { get; }
        public string Str { get; }

        public FileRecord(TNumber number, string str)
        {
            Number = number;
            Str = str;
        }

        int IComparable<FileRecord<TNumber>>.CompareTo(FileRecord<TNumber> other)
        {
            var numberCompareResult = Number.CompareTo(other.Number);
            if (numberCompareResult != 0)
            {
                return numberCompareResult;
            }

            return string.Compare(Str, other.Str, StringComparison.Ordinal);
        }

        bool IEquatable<FileRecord<TNumber>>.Equals(FileRecord<TNumber> other)
        {
            return Number.Equals(other.Number) && Str.Equals(other.Str);
        }
    }
}
