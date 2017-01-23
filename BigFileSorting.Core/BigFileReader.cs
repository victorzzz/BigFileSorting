using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BigFileSorting.Core.Exceptions;
using BigFileSorting.Core.Utils;

namespace BigFileSorting.Core
{
    internal class BigFileReader : IDisposable
    {
        internal struct ReadRecordResult
        {
            public FileRecord Record { get; }
            public int ReadBytes { get; }

            public ReadRecordResult(FileRecord record, int readBytes)
            {
                Record = record;
                ReadBytes = readBytes;
            }
        }

        private readonly FileStream m_FileStream;
        private readonly StreamReader m_StreamReader;
        private readonly CancellationToken m_CancellationToken;
        private readonly long m_SegmentSize;

        private bool disposedValue; // To detect redundant calls for 'Dispose'

        public BigFileReader(string filePath, long segmentSize, Encoding encoding, CancellationToken cancellationToken)
        {
            m_CancellationToken = cancellationToken;
            m_SegmentSize = segmentSize;

            m_FileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, Constants.FILE_BUFFER_SIZE, FileOptions.Asynchronous | FileOptions.SequentialScan);
            m_StreamReader = new StreamReader(m_FileStream, encoding);
        }

        public async Task<List<FileRecord>> ReadSegment()
        {
            long resultSize = 0;

            int aproxCapacity;
            checked
            {
                aproxCapacity = (int)(m_SegmentSize / Constants.APROXIMATE_RECORD_SIZE);
            }

            var result = new List<FileRecord>(capacity: aproxCapacity);
            while (true)
            {
                m_CancellationToken.ThrowIfCancellationRequested();

                var readRecordResult = await ReadRecord().ConfigureAwait(false);
                if (!readRecordResult.HasValue)
                {
                    return result;
                }
                else
                {
                    result.Add(readRecordResult.Value.Record);
                    resultSize += readRecordResult.Value.ReadBytes;
                    if (resultSize >= m_SegmentSize || result.Count > aproxCapacity)
                    {
                        return result;
                    }
                }
            }
        }

        private async Task<ReadRecordResult?> ReadRecord()
        {
            if (m_StreamReader.EndOfStream)
            {
                return null;
            }

            var line = await m_StreamReader.ReadLineAsync().ConfigureAwait(false);
            if (line == null)
            {
                return null;
            }

            int dotPosition;
            ulong parsedNumber = line.ParseULongToDelimiter('.', out dotPosition);

            return new ReadRecordResult(new FileRecord(parsedNumber, line.Substring(dotPosition + 1)), line.Length * 2);
        }

        #region IDisposable Support

        private void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    m_StreamReader.Dispose();
                    m_FileStream.Dispose();
                }

                disposedValue = true;
            }
        }

        void IDisposable.Dispose()
        {
            Dispose(true);
        }

        #endregion

    }
}
