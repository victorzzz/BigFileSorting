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
        private readonly Encoding m_Encoding;

        private bool disposedValue; // To detect redundant calls for 'Dispose'

        public BigFileWriter(string filePath, Encoding encoding, CancellationToken cancellationToken)
        {
            m_CancellationToken = cancellationToken;

            m_FileStream = new FileStream(filePath,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                Constants.FILE_BUFFER_SIZE, FileOptions.Asynchronous);

            m_StreamWriter = new StreamWriter(m_FileStream, encoding);

        }

        public async Task WriteOriginalSegmentAsync(IReadOnlyList<FileRecord> segment)
        {
            foreach(var record in segment)
            {
                await WriteOriginalRecordAsync(record).ConfigureAwait(false);
            }
        }

        public async Task WriteSegmentedFileRecordAsync(SegmentedFileRecord record)
        {
            string str;
            if (record.Str != null)
            {
                str = record.Str;
            } 
            else
            {
                str = m_Encoding.GetString(record.StrAsByteArray);
            }

            await m_StreamWriter.WriteLineAsync($"{record.Number}.{str}").ConfigureAwait(false);
        }

        private async Task WriteOriginalRecordAsync(FileRecord record)
        {
            await m_StreamWriter.WriteLineAsync($"{record.Number}.{record.Str}").ConfigureAwait(false);
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
