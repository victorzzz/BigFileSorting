using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;

namespace BigFileSorting.Core
{
    internal class TempSegmentedFile : IDisposable
    {
        private const int INDEX_FILE_BUFFER_SIZE = 1024 * 1024 * 1;

        private bool disposedValue = false; // To detect redundant calls

        private FileStream m_DataFile;
        private FileStream m_SegmentIndexFile;

        private bool m_ReadMode = false;
        private long m_NextSegmenPosition = 0;

        private readonly Encoding m_Encoding;

        private readonly string m_DataFilePah;
        private readonly string m_IndexFilePath;

        private CancellationToken m_CancellationToken;

        public TempSegmentedFile(string tempDir, Encoding encoding, CancellationToken cancellationToken)
        {
            m_Encoding = encoding;

            m_DataFilePah = Path.Combine(tempDir, Path.GetRandomFileName());

            m_DataFile = new FileStream(
                m_DataFilePah,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                Constants.FILE_BUFFER_SIZE,
                FileOptions.Asynchronous);

            m_IndexFilePath = Path.Combine(m_DataFilePah, ".indx");

            m_SegmentIndexFile = new FileStream(
                m_IndexFilePath,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                INDEX_FILE_BUFFER_SIZE,
                FileOptions.Asynchronous);

            m_ReadMode = false;

            m_CancellationToken = cancellationToken;
        }

        public async Task WriteSortedSegmentAsync(List<FileRecord> segment)
        {
            if (m_ReadMode)
            {
                throw new InvalidOperationException("Unexpected internal error! 'TempSegmentedFile.WriteSortedSegmentAsync' was called in read mode.");
            }

            foreach(var record in segment)
            {
                await WriteRecordAsync(record).ConfigureAwait(false);
            }

            await EndSegmentAsync().ConfigureAwait(false);
        }

        private async Task WriteRecordAsync(FileRecord record)
        {
            await m_DataFile.WriteAsync(BitConverter.GetBytes(record.Number), 0, 8, m_CancellationToken).ConfigureAwait(false);

            var strBytes = m_Encoding.GetBytes(record.Str);

            await m_DataFile.WriteAsync(BitConverter.GetBytes(strBytes.Length), 0, 4, m_CancellationToken).ConfigureAwait(false);
            await m_DataFile.WriteAsync(strBytes, 0, strBytes.Length, m_CancellationToken).ConfigureAwait(false);
        }

        public async Task WriteSegmentedFileRecordAsync(SegmentedFileRecord record)
        {
            await m_DataFile.WriteAsync(BitConverter.GetBytes(record.Number), 0, 8, m_CancellationToken).ConfigureAwait(false);
            await m_DataFile.WriteAsync(record.StrAsByteArray, 0, record.StrAsByteArray.Length, m_CancellationToken).ConfigureAwait(false);
        }

        public async Task EndSegmentAsync()
        {
            long pos = m_DataFile.Position;
            await m_SegmentIndexFile.WriteAsync(BitConverter.GetBytes(pos), 0, 8, m_CancellationToken).ConfigureAwait(false);
        }

        public async Task SwitchToReadModeAsync()
        {
            if (m_ReadMode)
            {
                throw new InvalidOperationException("Unexpected internal error! 'TempSegmentedFile.SwitchToReadModeAsync' was called in read mode.");
            }

            await Task.WhenAll(
                m_DataFile.FlushAsync(m_CancellationToken),
                m_SegmentIndexFile.FlushAsync(m_CancellationToken)
                ).ConfigureAwait(false);

            m_DataFile.Dispose();
            m_SegmentIndexFile.Dispose();

            m_DataFile = new FileStream(
                m_DataFilePah,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                Constants.FILE_BUFFER_SIZE,
                FileOptions.Asynchronous | FileOptions.SequentialScan | FileOptions.DeleteOnClose);

            m_SegmentIndexFile = new FileStream(
                m_IndexFilePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                INDEX_FILE_BUFFER_SIZE,
                FileOptions.Asynchronous | FileOptions.SequentialScan | FileOptions.DeleteOnClose);

            m_ReadMode = true;
        }

        public async Task<bool> NextSegmentAsync()
        {
            if (!m_ReadMode)
            {
                throw new InvalidOperationException("Unexpected internal error! 'TempSegmentedFile.NextSegmentAsync' was called in write mode.");
            }

            byte[] nextSegmentPosBuffer = new byte[8];
            var readBytes = await m_SegmentIndexFile.ReadAsync(nextSegmentPosBuffer, 0, 8, m_CancellationToken).ConfigureAwait(false);

            if (readBytes == 0)
            {
                return false;
            }

            if (readBytes != 8)
            {
                throw new InvalidOperationException("Unexpected internal error! Incorrect format of temporary segmented file.");
            }

            m_NextSegmenPosition = BitConverter.ToInt64(nextSegmentPosBuffer, 0);
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns>Returns null if end of segment is reached</returns>
        public async Task<SegmentedFileRecord?> ReadRecordToMergeAsync()
        {
            if (!m_ReadMode)
            {
                throw new InvalidOperationException("Unexpected internal error! 'TempSegmentedFile.ReadRecordToMergeAsync' was called in not read mode.");
            }

            if(m_DataFile.Position == m_NextSegmenPosition)
            {
                return null;
            }

            if (m_DataFile.Position > m_NextSegmenPosition)
            {
                throw new InvalidOperationException("Unexpected internal error! Unexpectedly Data File position is out of current segment.");
            }

            // read Number
            byte[] bufferNumber = new byte[8];
            var bytesRead = await m_DataFile.ReadAsync(bufferNumber, 0, 8, m_CancellationToken).ConfigureAwait(false);
            if (bytesRead != 8)
            {
                throw new InvalidOperationException("Unexpected internal error! Can't read Number for next record of temporary segmented file.");
            }

            // read string length
            byte[] bufferStringLength = new byte[4];
            bytesRead = await m_DataFile.ReadAsync(bufferNumber, 0, 4, m_CancellationToken).ConfigureAwait(false);
            if (bytesRead != 4)
            {
                throw new InvalidOperationException("Unexpected internal error! Can't read string length for the next record of temporary segmented file.");
            }

            int stringLength = BitConverter.ToInt32(bufferStringLength, 0);
            if (stringLength < 0)
            {
                throw new InvalidOperationException("Unexpected internal error! stringLength < 0 for the next record of temporary segmented file.");
            }

            // read string
            byte[] bufferString = new byte[stringLength];
            bytesRead = await m_DataFile.ReadAsync(bufferString, 0, stringLength, m_CancellationToken).ConfigureAwait(false);
            if (bytesRead != stringLength)
            {
                throw new InvalidOperationException("Unexpected internal error! Can't read String for the next record of temporary segmented file.");
            }

            return new SegmentedFileRecord(BitConverter.ToUInt64(bufferNumber, 0), bufferString);
        }

        private void DeleteFileSafe(string filePath)
        {
            try
            {
                if (File.Exists(filePath))
                {
                    File.Delete(filePath);
                }
            }
            catch (Exception)
            {

            }
        }

        #region IDisposable Support

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    m_DataFile.Dispose();
                    m_DataFile = null;
                    m_SegmentIndexFile.Dispose();
                    m_SegmentIndexFile = null;

                    if (!m_ReadMode)
                    {
                        // something went wrong
                        // delete files
                        DeleteFileSafe(m_DataFilePah);
                        DeleteFileSafe(m_IndexFilePath);
                    }
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
