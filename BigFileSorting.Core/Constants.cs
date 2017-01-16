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
        public const int MAX_NUMBER_STRING_REPRESENTAION_LENGTH = 256;

        public const int NUMBER_OF_PROGRESSIVE_READING_BUFFERS = 16;
    }
}
