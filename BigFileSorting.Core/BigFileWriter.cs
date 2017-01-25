using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;

namespace BigFileSorting.Core
{
    internal class BigFileWriter : IDisposable
    {
        private readonly FileStream m_FileStream;
        private readonly StreamWriter m_StreamWriter;
        private readonly CancellationToken m_CancellationToken;
        private readonly long m_SegmentSize;

        private bool disposedValue; // To detect redundant calls for 'Dispose'

        public BigFileWriter(string filePath, Encoding encoding, CancellationToken cancellationToken)
        {
            m_CancellationToken = cancellationToken;

            m_FileStream = new FileStream(filePath,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                Constants.FILE_BUFFER_SIZE, FileOptions.Asynchronous);

        }

        public async Task WriteOriginalSegmentAsync(IReadOnlyList<FileRecord> segment)
        {

        }

        public async Task WriteSegmentedFileRecordAsync(SegmentedFileRecord record)
        {
            await m_FileStream.WriteAsync(BitConverter.GetBytes(record.Number), 0, 8).ConfigureAwait(false);
            await 
        }

        #region IDisposable Support

        private void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    m_StreamWriter.Dispose();
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
