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
        internal static bool CheckRootPathUniqueness(IReadOnlyCollection<string> tempDirs, CancellationToken cancellationToken)
        {
            var concurrencyLevel = Environment.ProcessorCount * 2;
            var tempDirsWithRootPaths = new ConcurrentDictionary<string, string>(concurrencyLevel, tempDirs.Count);

            bool result = true;
            Parallel.ForEach(
                tempDirs,
                new ParallelOptions()
                {
                    CancellationToken = cancellationToken,
                    MaxDegreeOfParallelism = concurrencyLevel
                },
                (tempDir, state) =>
                {
                    var fullPath = Path.GetFullPath(tempDir);
                    var rootPath = Path.GetPathRoot(fullPath);
                    if (!tempDirsWithRootPaths.TryAdd(rootPath, tempDir))
                    {
                        result = false;
                        state.Break();
                    }
                });

            return result;
        }

        internal static IReadOnlyCollection<string> TempDirsToUse(IReadOnlyCollection<string> tempDirs, CancellationToken cancellationToken)
        {
            if (tempDirs.Count == 0)
            {
                throw new ArgumentException("At least one temporary directory should be provided.", nameof(tempDirs));
            }

            // Check temp directories. It should be placed on different devices
            if (tempDirs.Count > 1)
            {
                if (!TempDirectoryHelper.CheckRootPathUniqueness(tempDirs, cancellationToken))
                {
                    throw new InvalidOperationException("Temp dirs should have unique root paths!");
                }
            }
            else
            {
                // if the only one temporary directory provided thate we use two temp files
                tempDirs = new List<string>() { tempDirs.First(), tempDirs.First() };
            }

            return tempDirs;
        }
    }
}
