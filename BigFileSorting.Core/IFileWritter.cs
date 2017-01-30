using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigFileSorting.Core
{
    internal interface IFileWritter
    {
        Task WriteSegmentedFileRecordAsync(SegmentedFileRecord record);

        Task WriteOriginalFileRecordAsync(FileRecord record);
    }
}
