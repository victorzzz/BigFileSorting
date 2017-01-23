using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BigFileSorting.Core
{
    internal static class SegmentedFileMerger
    {
        public static async Task Merge(
            IReadOnlyList<TempSegmentedFile> sourceSegmentedFiles,
            TempSegmentedFile destinationSegmentedFile,
            Encoding encoding,
            CancellationToken cancellationToken)
        {
            var tempSegmentedFileRecords = new List<SegmentedFileRecord?>(capacity: sourceSegmentedFiles.Count);
            tempSegmentedFileRecords.AddRange(Enumerable.Repeat((SegmentedFileRecord?)null, sourceSegmentedFiles.Count));

            // initial record reading
            var tasks = Enumerable.Range(0, sourceSegmentedFiles.Count)
                .Select(async (i) =>
                {
                    var sourceSegmentedFile = sourceSegmentedFiles[i];
                    if ((await sourceSegmentedFile.NextSegmentAsync().ConfigureAwait(false)))
                    {
                        tempSegmentedFileRecords[i] = await sourceSegmentedFile.ReadRecordToMergeAsync().ConfigureAwait(false);
                    }
                });

            await Task.WhenAll(tasks).ConfigureAwait(false);

            while (true)
            {
                // find smallest item
                int smallestItemIndex = -1;
                SegmentedFileRecord? smallestRecord = null;
                 
                for (int i = 0; i < sourceSegmentedFiles.Count; ++i)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    
                    var record = tempSegmentedFileRecords[i];
                    if (!record.HasValue)
                    {
                        continue;
                    }

                    if (smallestRecord == null)
                    {
                        smallestRecord = record;
                        smallestItemIndex = i;
                    }
                    else
                    {
                        if(smallestRecord.Value.CompareTo(record.Value, encoding) == 1)
                        {
                            smallestRecord = record;
                            smallestItemIndex = i;
                        }
                    }
                }

                if (smallestItemIndex != -1)
                {
                    // write min record to the destination file
                    var writeTask = destinationSegmentedFile.WriteSegmentedFileRecordAsync(smallestRecord.Value);

                    // read next record from the source file where min record was found
                    var sourceSegmentedFileToread = sourceSegmentedFiles[smallestItemIndex];
                    var readTask = sourceSegmentedFileToread.ReadRecordToMergeAsync();

                    await Task.WhenAll(writeTask, readTask).ConfigureAwait(false);

                    tempSegmentedFileRecords[smallestItemIndex] = readTask.GetAwaiter().GetResult();
                }
                else
                {
                    await destinationSegmentedFile.EndSegmentAsync();
                    break;
                }
            }
        }
    }
}
