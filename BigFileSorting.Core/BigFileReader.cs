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
    internal class BigFileReader<TNumber> : IDisposable
        where TNumber : struct, IComparable<TNumber>, IEquatable<TNumber>
    {
        internal struct ReadRecordResult
        {
            public FileRecord<TNumber> Record { get; }
            public int ReadBytes { get; }

            public ReadRecordResult(FileRecord<TNumber> record, int readBytes)
            {
                Record = record;
                ReadBytes = readBytes;
            }
        }

        private readonly FileStream m_FileStream;
        private readonly StreamReader m_StreamReader;
        private readonly CancellationToken m_CancellationToken;
        private readonly int m_SegmentSize;

        private bool disposedValue; // To detect redundant calls for 'Dispose'

        public BigFileReader(string filePath, int bufferSize, int segmentSize, Encoding encoding, CancellationToken cancellationToken)
        {
            if (bufferSize < Constants.MIN_FILE_READING_BUFFER_SIZE)
            {
                throw new ArgumentOutOfRangeException(nameof(bufferSize));
            }

            m_CancellationToken = cancellationToken;
            m_SegmentSize = segmentSize;

            m_FileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize, FileOptions.Asynchronous | FileOptions.SequentialScan);
            m_StreamReader = new StreamReader(m_FileStream, encoding);
        }

        public async Task<List<FileRecord<TNumber>>> ReadSegment()
        {
            long resultSize = 0;

            var result = new List<FileRecord<TNumber>>(capacity: m_SegmentSize / Constants.APROXIMATE_RECORD_SIZE);
            while (true)
            {
                m_CancellationToken.ThrowIfCancellationRequested();

                var readRecordResult = await ReadRecord().ConfigureAwait(false);
                if (readRecordResult.ReadBytes == 0)
                {
                    return result;
                }
                else
                {
                    result.Add(readRecordResult.Record);
                    resultSize += readRecordResult.ReadBytes;
                    if (resultSize >= m_SegmentSize)
                    {
                        return result;
                    }
                }
            }
        }

        private async Task<ReadRecordResult> ReadRecord()
        {
            var line = await m_StreamReader.ReadLineAsync().ConfigureAwait(false);
            var dotIndex = line.IndexOf('.');
            if (dotIndex <= 0)
            {
                throw new InvalidFileException("Character '.' absent or placed in the first position");
            }

            var numberStrToParse = line.Substring(0, dotIndex);

            var number = default(TNumber);
            var parseResult = (bool)NumberMethodInfo<TNumber>.TryParseMethodInfo.Invoke(
                null,
                new object[] { numberStrToParse, number });

            if (!parseResult)
            {
                throw new InvalidFileException("Can't parse 'Number'");
            }

            var str = line.Substring(dotIndex + 1);

            return new ReadRecordResult(new FileRecord<TNumber>(number, str), line.Length * 2);
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
