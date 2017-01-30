using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigFileSorting.Core
{
    internal static class Constants
    {
        public const int APROXIMATE_RECORD_SIZE = 128;
        public const int FILE_BUFFER_SIZE = 1024 * 32;
        public const long DEFAULT_AVALIABLE_MEMORY = 1024L * 1024L * 1024L * 4L; // 4Gb
    }
}
