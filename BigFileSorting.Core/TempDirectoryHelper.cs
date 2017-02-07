using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;

namespace BigFileSorting.Core
{
    internal class TempDirectoryHelper
    {
        internal static IReadOnlyList<string> TempDirsToUse(IReadOnlyList<string> tempDirs)
        {
            if (tempDirs.Count == 0)
            {
                throw new ArgumentException("At least one temporary directory should be provided.", nameof(tempDirs));
            }

            // Check temp directories. It should be placed on different devices
            if (tempDirs.Count == 1)
            {
                // if the only one temporary directory provided than we use three temp files in the same directory
                tempDirs = new List<string> { tempDirs.First(), tempDirs.First() };
            }

            return tempDirs;
        }
    }
}
