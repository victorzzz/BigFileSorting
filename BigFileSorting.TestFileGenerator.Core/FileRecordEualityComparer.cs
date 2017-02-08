using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BigFileSorting.Core;

namespace BigFileSorting.TestFileGenerator.Core
{
    public class FileRecordEualityComparer : IEqualityComparer<FileRecord>
    {
        bool IEqualityComparer<FileRecord>.Equals(FileRecord x, FileRecord y)
        {
            return x.Number == y.Number && string.Equals(x.Str, y.Str, StringComparison.Ordinal);
        }

        int IEqualityComparer<FileRecord>.GetHashCode(FileRecord obj)
        {
            return obj.Number.GetHashCode() ^ obj.Str.GetHashCode();
        }
    }
}
