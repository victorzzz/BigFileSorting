using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BigFileSorting.Core.Exceptions;

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

        public BigFileReader(string filePath, int bufferSize, long segmentSize, Encoding encoding, CancellationToken cancellationToken)
        {
            m_CancellationToken = cancellationToken;
            m_SegmentSize = segmentSize;

            m_FileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize, FileOptions.Asynchronous | FileOptions.SequentialScan);
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

            //int size = System.Runtime.InteropServices.Marshal.SizeOf(typeof(FileRecord));

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
            var line = await m_StreamReader.ReadLineAsync().ConfigureAwait(false);
            if (line == null)
            {
                return null;
            }

            var dotIndex = line.IndexOf('.');
            if (dotIndex <= 0)
            {
                throw new InvalidFileException("Character '.' absent or placed in the first position");
            }

            var numberStrToParse = line.Substring(0, dotIndex);

            long parsedNumber;
            var parseResult = long.TryParse(numberStrToParse, out parsedNumber);
            if (!parseResult)
            {
                throw new InvalidFileException("Can't parse 'Number'");
            }

            var str = line.Substring(dotIndex + 1);

            return new ReadRecordResult(new FileRecord(parsedNumber, str), line.Length * 2);
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
