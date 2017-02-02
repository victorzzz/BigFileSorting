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
        private const int ReadLinesQueSize = 20;

        private readonly StreamReader m_StreamReader;
        private readonly CancellationToken m_CancellationToken;

        private readonly BlockingCollection<string> m_ReadLinesCollection;
        private readonly Task m_BackgroundReadingTask;

        private volatile bool m_EndOfFile;

        private bool disposedValue; // To detect redundant calls for 'Dispose'

        public BigFileReader(string filePath, Encoding encoding, CancellationToken cancellationToken)
        {
            m_CancellationToken = cancellationToken;

            var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, Constants.FILE_BUFFER_SIZE, FileOptions.SequentialScan);
            m_StreamReader = new StreamReader(fileStream, encoding, false, Constants.FILE_BUFFER_SIZE);

            m_ReadLinesCollection = new BlockingCollection<string>(ReadLinesQueSize);
            m_BackgroundReadingTask = Task.Factory.StartNew(
                ReadLines,
                m_CancellationToken,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
        }

        private void ReadLines()
        {
            while (true)
            {
                m_CancellationToken.ThrowIfCancellationRequested();

                if(m_ReadLinesCollection.IsAddingCompleted)
                {
                    break;
                }

                var str = m_StreamReader.ReadLine();
                m_ReadLinesCollection.Add(str, m_CancellationToken);
                if (str == null)
                {
                    m_EndOfFile = true;
                    break;
                }
            }
        }

        public bool EndOfFile()
        {
            return m_EndOfFile;
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
            if(m_EndOfFile || m_ReadLinesCollection.IsCompleted)
            {
                return null;
            }

            string line;
            try
            {
                line = m_ReadLinesCollection.Take(m_CancellationToken);
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
            if (!disposedValue)
            {
                if (disposing)
                {
                    m_ReadLinesCollection.CompleteAdding();
                    m_BackgroundReadingTask.GetAwaiter().GetResult();

                    m_StreamReader.Dispose();
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
