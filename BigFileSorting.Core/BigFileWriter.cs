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

        private bool m_DisposedValue; // To detect redundant calls for 'Dispose'

        public BigFileWriter(string filePath, Encoding encoding, CancellationToken cancellationToken)
        {
            m_CancellationToken = cancellationToken;
            m_Encoding = encoding;

            var fileStream = new FileStream(filePath,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                Constants.FILE_BUFFER_SIZE, FileOptions.None);

            m_StreamWriter = new StreamWriter(fileStream, encoding, Constants.FILE_BUFFER_SIZE);
            m_ProactiveTaskRunner = new ProactiveTaskRunner(cancellationToken);
        }
        public void WriteOriginalSegment(IReadOnlyList<FileRecord> segmen)
        {
            m_ProactiveTaskRunner.WaitForProactiveTask();
            m_ProactiveTaskRunner.StartProactiveTask(() => WriteOriginalSegmentImple(segmen));
        }

        private void WriteOriginalSegmentImple(IReadOnlyList<FileRecord> segment)
        {
            foreach(var record in segment)
            {
                m_CancellationToken.ThrowIfCancellationRequested();
                WriteOriginalFileRecordImpl(record);
            }
        }

        public void WriteTempFileRecord(TempFileRecord record)
        {
            m_ProactiveTaskRunner.WaitForProactiveTask();
            m_ProactiveTaskRunner.StartProactiveTask(() => WriteTempFileRecordImpl(record));
        }

        private void WriteTempFileRecordImpl(TempFileRecord record)
        {
            var str = record.GetStr(m_Encoding);
            m_StreamWriter.WriteLine($"{record.Number}.{str}");
            record.ClearStr();
        }

        public void WriteOriginalFileRecord(FileRecord record)
        {
            m_ProactiveTaskRunner.WaitForProactiveTask();
            m_ProactiveTaskRunner.StartProactiveTask(() => WriteOriginalFileRecordImpl(record));
        }

        private void WriteOriginalFileRecordImpl(FileRecord record)
        {
            m_StreamWriter.WriteLine($"{record.Number}.{record.Str}");
            record.ClearStr();
        }

        public void FlushDataAndDisposeFiles()
        {
            m_ProactiveTaskRunner.WaitForProactiveTask();
            FlushDataAndDisposeFilesImpl();
        }

        private void FlushDataAndDisposeFilesImpl()
        {
            if (m_StreamWriter != null)
            {
                m_StreamWriter.Flush();
                m_StreamWriter.Dispose();
                m_StreamWriter = null;
            }
        }

        #region IDisposable Support

        private void Dispose(bool disposing)
        {
            if (!m_DisposedValue)
            {
                if (disposing)
                {
                    FlushDataAndDisposeFilesImpl();
                }

                m_DisposedValue = true;
            }
        }

        void IDisposable.Dispose()
        {
            Dispose(true);
        }

        #endregion

    }
}
