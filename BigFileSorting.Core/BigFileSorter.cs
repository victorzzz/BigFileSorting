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
    public class BigFileSorter<TNumber> where TNumber : struct, IComparable<TNumber>, IEquatable<TNumber>
    {
        public async Task Sort(
            string sourceFilePath,
            string targetDir,
            IReadOnlyCollection<string> tempDirs,
            int fileReadBufferSize,
            int segmentSize,
            Encoding encoding,
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

            // check temp dirs
            tempDirs = TempDirectoryHelper.TempDirsToUse(tempDirs, cancellationToken);

            //read source file, devide into segments, sort segments in the memory, put segments into temp files on temp directories
            using (var fileReader = new BigFileReader<TNumber>(sourceFilePath, fileReadBufferSize, segmentSize, encoding, cancellationToken))
            {
                var segment = await fileReader.ReadSegment();
                segment.Sort();
            } 
        }
    }
}
