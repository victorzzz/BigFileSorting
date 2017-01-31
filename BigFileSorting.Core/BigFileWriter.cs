using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;

namespace BigFileSorting.Core
{
    internal class BigFileWriter : IFileWritter, IDisposable
    {
        private readonly CancellationToken m_CancellationToken;
        private readonly Encoding m_Encoding;
        private StreamWriter m_StreamWriter;

        private readonly ProactiveTaskRunner m_ProactiveTaskRunner;

        private bool disposedValue; // To detect redundant calls for 'Dispose'

        public BigFileWriter(string filePath, Encoding encoding, CancellationToken cancellationToken)
        {
            m_CancellationToken = cancellationToken;
            m_Encoding = encoding;

            var fileStream = new FileStream(filePath,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                Constants.FILE_BUFFER_SIZE, FileOptions.Asynchronous);

            m_StreamWriter = new StreamWriter(fileStream, encoding, Constants.FILE_BUFFER_SIZE);
            m_ProactiveTaskRunner = new ProactiveTaskRunner(cancellationToken);
        }
        public async Task WriteOriginalSegmentAsync(IReadOnlyList<FileRecord> segmen)
        {
            await m_ProactiveTaskRunner.WaitForProactiveTaskAsync().ConfigureAwait(false);
            m_ProactiveTaskRunner.StartProactiveTask(async () => await WriteOriginalSegmentImpleAsync(segmen).ConfigureAwait(false));
        }

        private async Task WriteOriginalSegmentImpleAsync(IReadOnlyList<FileRecord> segment)
        {
            foreach(var record in segment)
            {
                m_CancellationToken.ThrowIfCancellationRequested();

                await WriteOriginalFileRecordImplAsync(record).ConfigureAwait(false);

                record.ClearStr();
            }
        }

        public async Task WriteSegmentedFileRecordAsync(SegmentedFileRecord record)
        {
            await m_ProactiveTaskRunner.WaitForProactiveTaskAsync().ConfigureAwait(false);
            m_ProactiveTaskRunner.StartProactiveTask(async () => await WriteSegmentedFileRecordImplAsync(record).ConfigureAwait(false));
        }

        private async Task WriteSegmentedFileRecordImplAsync(SegmentedFileRecord record)
        {
            string str = record.GetStr(m_Encoding);
            await m_StreamWriter.WriteLineAsync($"{record.Number}.{str}").ConfigureAwait(false);

            record.ClearStr();
        }

        public async Task WriteOriginalFileRecordAsync(FileRecord record)
        {
            await m_ProactiveTaskRunner.WaitForProactiveTaskAsync().ConfigureAwait(false);
            m_ProactiveTaskRunner.StartProactiveTask(async () => await WriteOriginalFileRecordImplAsync(record).ConfigureAwait(false));
        }

        private async Task WriteOriginalFileRecordImplAsync(FileRecord record)
        {
            await m_StreamWriter.WriteLineAsync($"{record.Number}.{record.Str}").ConfigureAwait(false);
        }

        public async Task FlushDataAndDisposeFilesAsync()
        {
            await m_ProactiveTaskRunner.WaitForProactiveTaskAsync().ConfigureAwait(false);
            await FlushDataAndDisposeFilesImplAsync().ConfigureAwait(false);
        }

        private async Task FlushDataAndDisposeFilesImplAsync()
        {
            if (m_StreamWriter != null)
            {
                await m_StreamWriter.FlushAsync().ConfigureAwait(false);
                m_StreamWriter.Dispose();
                m_StreamWriter = null;
            }
        }

        #region IDisposable Support

        private void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    FlushDataAndDisposeFilesImplAsync().Wait();
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
