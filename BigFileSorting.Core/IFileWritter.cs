using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigFileSorting.Core
{
    internal interface IFileWritter
    {
        void WriteTempFileRecord(TempFileRecord record);

        void WriteOriginalFileRecord(FileRecord record);
    }
}
