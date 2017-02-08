using System;
using System.Collections.Concurrent;
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
        private bool m_DisposedValue = false; // To detect redundant calls

        private readonly string m_TempDir;

        private BlockingCollection<TempFileRecord> m_ReadingCollection;
        private BlockingCollection<byte[]> m_WritingCollection;

        private Task m_BackgroundTask;

        private Stream m_DataFileStream;

        private string m_DataFilePah;
        private readonly Encoding m_Encoding;
        private CancellationTokenSource m_CancellationTokenSource;
        private bool m_ReadMode;

        private readonly byte[] m_BufferNumber = new byte[8];
        private readonly byte[] m_BufferStringLength = new byte[4];

        public TempFile(string tempDir, Encoding encoding, CancellationToken cancellationToken)
        {
            m_TempDir = tempDir;
            m_Encoding = encoding;
            m_CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        }

        public void SwitchToNewFile()
        {
            if (m_DataFilePah != null)
            {
                if ((!m_ReadMode || m_ReadingCollection == null || m_WritingCollection != null))
                {
                    throw new InvalidOperationException("Unexpected internal error! 'TempFile.SwitchToNewFile' was called in write mode.");
                }

                m_ReadingCollection.CompleteAdding();
                m_BackgroundTask.GetAwaiter().GetResult();

                FlushDataAndDisposeFilesImpl();
                DeleteFileSafe(m_DataFilePah);
            }

            m_DataFilePah = Path.Combine(m_TempDir, Path.GetRandomFileName());
            m_DataFileStream = new BufferedStream(
                new FileStream(
                    m_DataFilePah,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.None,
                    Constants.FILE_BUFFER_SIZE,
                    FileOptions.None));
            m_ReadMode = false;

            m_ReadingCollection = null;
            m_WritingCollection = new BlockingCollection<byte[]>(Constants.BACKGROUND_FILEOPERATIONS_QUEUE_SIZE);

            m_BackgroundTask = Task.Factory.StartNew(Writing,
                m_CancellationTokenSource.Token,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
        }

        public void WriteOriginalSegment(IReadOnlyList<FileRecord> segment)
        {
            if (m_ReadMode)
            {
                throw new InvalidOperationException("Unexpected internal error! 'TempFile.WriteSortedSegment' was called in read mode.");
            }

            foreach (var record in segment)
            {
                m_CancellationTokenSource.Token.ThrowIfCancellationRequested();
                WriteOriginalFileRecord(record);
            }
        }

        public void WriteOriginalFileRecord(FileRecord record)
        {
            var strBytes = m_Encoding.GetBytes(record.Str);
            var dataToWrite = new byte[12 + strBytes.Length];
            Buffer.BlockCopy(BitConverter.GetBytes(record.Number), 0, dataToWrite, 0, 8);
            Buffer.BlockCopy(BitConverter.GetBytes(strBytes.Length), 0, dataToWrite, 8, 4);
            Buffer.BlockCopy(strBytes, 0, dataToWrite, 12, strBytes.Length);

            record.ClearStr();

            try
            {
                m_WritingCollection.Add(dataToWrite);
            }
            catch(InvalidOperationException)
            {
                throw new InvalidOperationException($"Unexpected problem on writing file '{m_DataFilePah}'");
            }
        }

        public void WriteTempFileRecord(TempFileRecord record)
        {
            var dataToWrite = new byte[12 + record.StrAsByteArray.Length];
            Buffer.BlockCopy(BitConverter.GetBytes(record.Number), 0, dataToWrite, 0, 8);
            Buffer.BlockCopy(BitConverter.GetBytes(record.StrAsByteArray.Length), 0, dataToWrite, 8, 4);
            Buffer.BlockCopy(record.StrAsByteArray, 0, dataToWrite, 12, record.StrAsByteArray.Length);

            record.ClearStr();

            try
            {
                m_WritingCollection.Add(dataToWrite);
            }
            catch (InvalidOperationException)
            {
                throw new InvalidOperationException($"Unexpected problem on writing file '{m_DataFilePah}'");
            }
        }

        public void SwitchToReadMode()
        {
            if (m_ReadMode)
            {
                throw new InvalidOperationException("Unexpected internal error! 'TempFile.SwitchToReadMode' was called in read mode.");
            }

            m_WritingCollection.CompleteAdding();
            m_BackgroundTask.GetAwaiter().GetResult();

            FlushDataAndDisposeFilesImpl();

            m_DataFileStream = new BufferedStream(
                new FileStream(
                    m_DataFilePah,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read,
                    Constants.FILE_BUFFER_SIZE,
                    FileOptions.SequentialScan | FileOptions.DeleteOnClose));

            m_ReadMode = true;

            m_WritingCollection = null;
            m_ReadingCollection = new BlockingCollection<TempFileRecord>(Constants.BACKGROUND_FILEOPERATIONS_QUEUE_SIZE);

            m_BackgroundTask = Task.Factory.StartNew(Reading,
                m_CancellationTokenSource.Token,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
        }

        public TempFileRecord? ReadRecordToMerge()
        {
            TempFileRecord? result = null;

            try
            {
                result = m_ReadingCollection.Take(m_CancellationTokenSource.Token);
            }
            catch(InvalidOperationException)
            {

            }

            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns>Returns null if end of segment is reached</returns>
        private TempFileRecord? ReadRecordToMergeImpl()
        {
            if (!m_ReadMode)
            {
                throw new InvalidOperationException("Unexpected internal error! 'TempFile.ReadRecordToMerge' was called in not read mode.");
            }

            // read Number
            var bytesRead = m_DataFileStream.Read(m_BufferNumber, 0, 8);

            if (bytesRead == 0)
            {
                return null;
            }

            if (bytesRead != 8)
            {
                throw new InvalidOperationException("Unexpected internal error! Can't read Number for next record of temporary segmented file.");
            }

            // read string length
            bytesRead = m_DataFileStream.Read(m_BufferStringLength, 0, 4);
            if (bytesRead != 4)
            {
                throw new InvalidOperationException("Unexpected internal error! Can't read string length for the next record of temporary segmented file.");
            }

            int stringLength = BitConverter.ToInt32(m_BufferStringLength, 0);
            if (stringLength < 0)
            {
                throw new InvalidOperationException("Unexpected internal error! stringLength < 0 for the next record of temporary segmented file.");
            }

            // read string
            byte[] bufferString = new byte[stringLength];
            bytesRead = m_DataFileStream.Read(bufferString, 0, stringLength);
            if (bytesRead != stringLength)
            {
                throw new InvalidOperationException("Unexpected internal error! Can't read String for the next record of temporary segmented file.");
            }

            return new TempFileRecord(BitConverter.ToUInt64(m_BufferNumber, 0), bufferString);
        }

        public void FlushDataAndDisposeFiles()
        {
            FlushDataAndDisposeFilesImpl();
        }

        private  void FlushDataAndDisposeFilesImpl()
        {
            if (m_DataFileStream != null)
            {
                m_DataFileStream.Flush();
                m_DataFileStream.Dispose();
                m_DataFileStream = null;
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

        private void Reading()
        {
            try
            {
                while (true)
                {
                    m_CancellationTokenSource.Token.ThrowIfCancellationRequested();

                    if (m_ReadingCollection.IsAddingCompleted)
                    {
                        break;
                    }

                    var readResult = ReadRecordToMergeImpl();
                    if (!readResult.HasValue)
                    {
                        m_ReadingCollection.CompleteAdding();
                        break;
                    }
                    else
                    {
                        m_ReadingCollection.Add(readResult.Value, m_CancellationTokenSource.Token);
                    }
                }
            }
            catch(Exception)
            {
                m_ReadingCollection.CompleteAdding();
                throw;
            }
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

                    byte[] objectToWrite;
                    try
                    {
                        objectToWrite = m_WritingCollection.Take(m_CancellationTokenSource.Token);
                    }
                    catch (InvalidOperationException)
                    {
                        break;
                    }

                    m_DataFileStream.Write(objectToWrite, 0, objectToWrite.Length);
                }
            }
            catch (Exception)
            {
                m_WritingCollection.CompleteAdding();
                throw;
            }
        }


        #region IDisposable Support

        protected virtual void Dispose(bool disposing)
        {
            if (!m_DisposedValue)
            {
                if (disposing)
                {
                    m_CancellationTokenSource.Cancel();
                    FlushDataAndDisposeFiles();

                    if (!m_ReadMode)
                    {
                        // something went wrong
                        // delete files
                        DeleteFileSafe(m_DataFilePah);
                    }
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
