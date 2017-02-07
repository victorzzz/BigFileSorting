using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
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
        private readonly StreamReader m_StreamReader;
        private readonly CancellationTokenSource m_CancellationTokenSource;

        private readonly BlockingCollection<string> m_ReadLinesCollection;
        private readonly Task m_BackgroundReadingTask;

        private bool m_DisposedValue; // To detect redundant calls for 'Dispose'

        public BigFileReader(string filePath, Encoding encoding, CancellationToken cancellationToken)
        {
            m_CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, Constants.FILE_BUFFER_SIZE, FileOptions.SequentialScan);
            m_StreamReader = new StreamReader(fileStream, encoding, false, Constants.FILE_BUFFER_SIZE);

            m_ReadLinesCollection = new BlockingCollection<string>(Constants.BACKGROUND_FILEOPERATIONS_QUEUE_SIZE);
            m_BackgroundReadingTask = Task.Factory.StartNew(
                ReadLines,
                m_CancellationTokenSource.Token,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
        }

        private void ReadLines()
        {
            while (true)
            {
                m_CancellationTokenSource.Token.ThrowIfCancellationRequested();

                if(m_ReadLinesCollection.IsAddingCompleted)
                {
                    break;
                }

                var str = m_StreamReader.ReadLine();
                if (str == null)
                {
                    m_ReadLinesCollection.CompleteAdding();
                    break;
                }

                m_ReadLinesCollection.Add(str, m_CancellationTokenSource.Token);
            }
        }

        public bool EndOfFile()
        {
            return m_ReadLinesCollection.IsCompleted;
        }

        public FileRecord? ReadRecord()
        {
            var line = ReadLine();
            if (line == null)
            {
                return null;
            }

            int dotPosition;
            ulong parsedNumber = line.ParseULongToDelimiter('.', out dotPosition);

            return new FileRecord(parsedNumber, line.Substring(dotPosition + 1));
        }  

        private string ReadLine()
        {
            if(m_ReadLinesCollection.IsCompleted)
            {
                return null;
            }

            string line;
            try
            {
                line = m_ReadLinesCollection.Take(m_CancellationTokenSource.Token);
            }
            catch(InvalidOperationException)
            {
                return null;
            }

            return line;
        }

        #region IDisposable Support

        private void Dispose(bool disposing)
        {
            if (!m_DisposedValue)
            {
                if (disposing)
                {
                    m_CancellationTokenSource.Cancel();
                    m_BackgroundReadingTask.GetAwaiter().GetResult();

                    m_StreamReader.Dispose();
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
