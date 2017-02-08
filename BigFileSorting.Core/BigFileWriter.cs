using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;

namespace BigFileSorting.Core
{
    internal class BigFileWriter : IFileWritter, IDisposable
    {
        private readonly CancellationTokenSource m_CancellationTokenSource;
        private readonly Encoding m_Encoding;
        private StreamWriter m_StreamWriter;

        private readonly BlockingCollection<string> m_WritingCollection;

        private readonly Task m_BackgroundTask;

        private bool m_DisposedValue; // To detect redundant calls for 'Dispose'

        public BigFileWriter(string filePath, Encoding encoding, CancellationToken cancellationToken)
        {
            m_CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            m_Encoding = encoding;

            var fileStream = new FileStream(filePath,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                Constants.FILE_BUFFER_SIZE, FileOptions.None);

            m_StreamWriter = new StreamWriter(fileStream, encoding, Constants.FILE_BUFFER_SIZE);

            m_WritingCollection = new BlockingCollection<string>(Constants.BACKGROUND_FILEOPERATIONS_QUEUE_SIZE);
            m_BackgroundTask = Task.Factory.StartNew(Writing,
                m_CancellationTokenSource.Token,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
        }
        public void WriteOriginalSegment(IReadOnlyList<FileRecord> segment)
        {
            foreach (var record in segment)
            {
                m_CancellationTokenSource.Token.ThrowIfCancellationRequested();
                WriteOriginalFileRecord(record);
            }
        }

        public void WriteTempFileRecord(TempFileRecord record)
        {
            var str = record.GetStr(m_Encoding);
            record.ClearStr();

            try
            {
                m_WritingCollection.Add($"{record.Number}.{str}");
            }
            catch (InvalidOperationException)
            {
                throw new InvalidOperationException("Unexpected problem on writing the target file");
            }
        }

        public void WriteOriginalFileRecord(FileRecord record)
        {
            try
            {
                m_WritingCollection.Add($"{record.Number}.{record.Str}");
            }
            catch (InvalidOperationException)
            {
                throw new InvalidOperationException("Unexpected problem on writing the target file");
            }

            record.ClearStr();
        }

        private void Writing()
        {
            try
            {
                while (true)
                {
                    m_CancellationTokenSource.Token.ThrowIfCancellationRequested();

                    if (m_WritingCollection.IsAddingCompleted)
                    {
                        break;
                    }

                    string stringToWrite;
                    try
                    {
                        stringToWrite = m_WritingCollection.Take(m_CancellationTokenSource.Token);
                    }
                    catch (InvalidOperationException)
                    {
                        break;
                    }

                    m_StreamWriter.WriteLine(stringToWrite);
                }
            }
            catch (Exception)
            {
                m_WritingCollection.CompleteAdding();
                throw;
            }
        }


        public void FlushDataAndDisposeFiles()
        {
            m_WritingCollection.CompleteAdding();
            m_BackgroundTask.GetAwaiter().GetResult();

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
                    m_CancellationTokenSource.Cancel();
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
