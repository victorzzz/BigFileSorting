using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;

namespace BigFileSorting.Core
{
    internal class TempFile : IFileWritter, IDisposable
    {
        private bool disposedValue = false; // To detect redundant calls

        private readonly string m_TempDir;

        private FileStream m_DataFile;
        private string m_DataFilePah;
        private readonly Encoding m_Encoding;
        private CancellationToken m_CancellationToken;
        private bool m_ReadMode;

        private ProactiveTaskRunner m_ProactiveTaskRunner;

        public TempFile(string tempDir, Encoding encoding, CancellationToken cancellationToken)
        {
            m_TempDir = tempDir;
            m_Encoding = encoding;
            m_CancellationToken = cancellationToken;
            m_ProactiveTaskRunner = new ProactiveTaskRunner(cancellationToken);
        }

        public async Task SwitchToNewFileAsync()
        {
            await m_ProactiveTaskRunner.WaitForProactiveTaskAsync().ConfigureAwait(false);

            if (m_DataFilePah != null && !m_ReadMode)
            {
                throw new InvalidOperationException("Unexpected internal error! 'TempSegmentedFile.SwitchToNewFileAsync' was called in write mode.");
            }

            await FlushDataAndDisposeFilesImplAsync().ConfigureAwait(false);

            DeleteFileSafe(m_DataFilePah);

            m_DataFilePah = Path.Combine(m_TempDir, Path.GetRandomFileName());

            m_DataFile = new FileStream(
                m_DataFilePah,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                Constants.FILE_BUFFER_SIZE,
                FileOptions.Asynchronous);

            m_ReadMode = false;
        }

        public async Task WriteSortedSegmentAsync(IReadOnlyList<FileRecord> segment)
        {
            await m_ProactiveTaskRunner.WaitForProactiveTaskAsync().ConfigureAwait(false);
            m_ProactiveTaskRunner.StartProactiveTask(async () => await WriteSortedSegmentImplAsync(segment).ConfigureAwait(false));
        }

        private async Task WriteSortedSegmentImplAsync(IReadOnlyList<FileRecord> segment)
        {
            if (m_ReadMode)
            {
                throw new InvalidOperationException("Unexpected internal error! 'TempSegmentedFile.WriteSortedSegmentAsync' was called in read mode.");
            }

            foreach(var record in segment)
            {
                m_CancellationToken.ThrowIfCancellationRequested();
                await WriteOriginalFileRecordImplAsync(record).ConfigureAwait(false);
            }
        }

        public async Task WriteOriginalFileRecordAsync(FileRecord record)
        {
            await m_ProactiveTaskRunner.WaitForProactiveTaskAsync().ConfigureAwait(false);
            m_ProactiveTaskRunner.StartProactiveTask(async () => await WriteOriginalFileRecordImplAsync(record).ConfigureAwait(false));
        }

        private async Task WriteOriginalFileRecordImplAsync(FileRecord record)
        {
            await m_DataFile.WriteAsync(BitConverter.GetBytes(record.Number), 0, 8, m_CancellationToken).ConfigureAwait(false);

            var strBytes = m_Encoding.GetBytes(record.Str);

            await m_DataFile.WriteAsync(BitConverter.GetBytes(strBytes.Length), 0, 4, m_CancellationToken).ConfigureAwait(false);
            await m_DataFile.WriteAsync(strBytes, 0, strBytes.Length, m_CancellationToken).ConfigureAwait(false);
        }

        public async Task WriteSegmentedFileRecordAsync(SegmentedFileRecord record)
        {
            await m_ProactiveTaskRunner.WaitForProactiveTaskAsync().ConfigureAwait(false);
            m_ProactiveTaskRunner.StartProactiveTask(async () => await WriteSegmentedFileRecordImplAsync(record).ConfigureAwait(false));
        }

        private async Task WriteSegmentedFileRecordImplAsync(SegmentedFileRecord record)
        {
            await m_DataFile.WriteAsync(BitConverter.GetBytes(record.Number), 0, 8, m_CancellationToken).ConfigureAwait(false);
            await m_DataFile.WriteAsync(BitConverter.GetBytes(record.StrAsByteArray.Length), 0, 4, m_CancellationToken).ConfigureAwait(false);
            await m_DataFile.WriteAsync(record.StrAsByteArray, 0, record.StrAsByteArray.Length, m_CancellationToken).ConfigureAwait(false);
        }

        public async Task SwitchToReadModeAsync()
        {
            await m_ProactiveTaskRunner.WaitForProactiveTaskAsync().ConfigureAwait(false);
            await SwitchToReadModeImplAsync().ConfigureAwait(false);

            m_ProactiveTaskRunner.StartProactiveTask<SegmentedFileRecord?>(async () => await ReadRecordToMergeImplAsync().ConfigureAwait(false));
        }

        private async Task SwitchToReadModeImplAsync()
        {
            if (m_ReadMode)
            {
                throw new InvalidOperationException("Unexpected internal error! 'TempSegmentedFile.SwitchToReadModeAsync' was called in read mode.");
            }

            await FlushDataAndDisposeFilesImplAsync().ConfigureAwait(false);

            m_DataFile = new FileStream(
                m_DataFilePah,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                Constants.FILE_BUFFER_SIZE,
                FileOptions.Asynchronous | FileOptions.SequentialScan | FileOptions.DeleteOnClose);

            m_ReadMode = true;
        }

        public async Task<SegmentedFileRecord?> ReadRecordToMergeAsync()
        {
            var result = await m_ProactiveTaskRunner.WaitForProactiveTaskAsync<SegmentedFileRecord?>().ConfigureAwait(false);

            if (result.HasValue)
            {
                m_ProactiveTaskRunner.StartProactiveTask<SegmentedFileRecord?>(async () => await ReadRecordToMergeImplAsync().ConfigureAwait(false));
            }

            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns>Returns null if end of segment is reached</returns>
        private async Task<SegmentedFileRecord?> ReadRecordToMergeImplAsync()
        {
            if (!m_ReadMode)
            {
                throw new InvalidOperationException("Unexpected internal error! 'TempSegmentedFile.ReadRecordToMergeAsync' was called in not read mode.");
            }

            // read Number
            byte[] bufferNumber = new byte[8];
            var bytesRead = await m_DataFile.ReadAsync(bufferNumber, 0, 8, m_CancellationToken).ConfigureAwait(false);

            if (bytesRead == 0)
            {
                return null;
            }

            if (bytesRead != 8)
            {
                throw new InvalidOperationException("Unexpected internal error! Can't read Number for next record of temporary segmented file.");
            }

            // read string length
            byte[] bufferStringLength = new byte[4];
            bytesRead = await m_DataFile.ReadAsync(bufferStringLength, 0, 4, m_CancellationToken).ConfigureAwait(false);
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

        public async Task FlushDataAndDisposeFilesAsync()
        {
            await m_ProactiveTaskRunner.WaitForProactiveTaskAsync().ConfigureAwait(false);
            await FlushDataAndDisposeFilesImplAsync().ConfigureAwait(false);
        }

        private  async Task FlushDataAndDisposeFilesImplAsync()
        {
            if (m_DataFile != null)
            {
                await m_DataFile.FlushAsync(m_CancellationToken).ConfigureAwait(false);
                m_DataFile.Dispose();
                m_DataFile = null;
            }
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
                    FlushDataAndDisposeFilesAsync().Wait();

                    if (!m_ReadMode)
                    {
                        // something went wrong
                        // delete files
                        DeleteFileSafe(m_DataFilePah);
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
