using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Numerics;

namespace BigFileSorting.Core
{
    public class BigFileSorter<TNumber> where TNumber : struct, IComparable<TNumber>
    {
        public async Task Sort(
            string sourceFilePath,
            string targetDir,
            IReadOnlyCollection<string> tempDirs,
            int fileReadBufferSize,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(sourceFilePath))
            {
                throw new ArgumentNullException(nameof(sourceFilePath));
            }

            if (string.IsNullOrWhiteSpace(targetDir))
            {
                throw new ArgumentNullException(nameof(targetDir));
            }

            if (tempDirs == null)
            {
                throw new ArgumentNullException(nameof(tempDirs));
            }

            if (cancellationToken == null)
            {
                throw new ArgumentNullException(nameof(cancellationToken));
            }

            // check temp dirs
            tempDirs = TempDirectoryHelper.TempDirsToUse(tempDirs, cancellationToken);

            //read source file, devide into segments, sort segments in the memory, put segments into temp files on temp directories
            using (var fileReader = new FileReader<TNumber>(sourceFilePath, fileReadBufferSize))
            {

            } 
        }
    }
}
