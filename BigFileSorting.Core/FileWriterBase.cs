using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace BigFileSorting.Core
{
    internal abstract class FileWriterBase
    {
        abstract protected void WriteTempFileRecordImpl(TempFileRecord record);

        abstract protected void WriteOriginalFileRecordImpl(FileRecord record);

        protected void Writing(CancellationToken cancellationToken, BlockingCollection<object> writingCollection)
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (writingCollection.IsAddingCompleted)
                {
                    break;
                }

                object objectToWrite;
                try
                {
                    objectToWrite = writingCollection.Take(cancellationToken);
                }
                catch (InvalidOperationException)
                {
                    break;
                }

                if (objectToWrite is FileRecord)
                {
                    var record = (FileRecord)objectToWrite;
                    WriteOriginalFileRecordImpl(record);
                }
                else if (objectToWrite is TempFileRecord)
                {
                    var tempFileRecord = (TempFileRecord)objectToWrite;
                    WriteTempFileRecordImpl(tempFileRecord);
                }
                else
                {
                    throw new InvalidOperationException("Unexpected item in the WritingCollection");
                }
            }
        }

    }
}
