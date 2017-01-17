using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigFileSorting.Core
{
    internal static class Constants
    {
        public const int MIN_FILE_READING_BUFFER_SIZE = 1024 * 1024 * 64; // 64 Mb
        public const int APROXIMATE_RECORD_SIZE = 128;

    }
}
