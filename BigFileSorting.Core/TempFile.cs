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

        private readonly BlockingCollection<>

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

        public void SwitchToNewFile()
        {
            m_ProactiveTaskRunner.WaitForProactiveTask();

            if (m_DataFilePah != null && !m_ReadMode)
            {
                throw new InvalidOperationException("Unexpected internal error! 'TempFile.SwitchToNewFile' was called in write mode.");
            }

            FlushDataAndDisposeFilesImpl();

            DeleteFileSafe(m_DataFilePah);

            m_DataFilePah = Path.Combine(m_TempDir, Path.GetRandomFileName());

            m_DataFile = new FileStream(
                m_DataFilePah,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                Constants.FILE_BUFFER_SIZE,
                FileOptions.None);

            m_ReadMode = false;
        }

        public void WriteSortedSegment(IReadOnlyList<FileRecord> segment)
        {
            m_ProactiveTaskRunner.WaitForProactiveTask();
            WriteSortedSegmentImpl(segment);
        }

        private void WriteSortedSegmentImpl(IReadOnlyList<FileRecord> segment)
        {
            if (m_ReadMode)
            {
                throw new InvalidOperationException("Unexpected internal error! 'TempFile.WriteSortedSegment' was called in read mode.");
            }

            foreach(var record in segment)
            {
                m_CancellationToken.ThrowIfCancellationRequested();
                WriteOriginalFileRecordImpl(record);
            }
        }

        public void WriteOriginalFileRecord(FileRecord record)
        {
            m_ProactiveTaskRunner.WaitForProactiveTask();
            m_ProactiveTaskRunner.StartProactiveTask(() => WriteOriginalFileRecordImpl(record));
        }

        private void WriteOriginalFileRecordImpl(FileRecord record)
        {
            m_DataFile.Write(BitConverter.GetBytes(record.Number), 0, 8);

            var strBytes = m_Encoding.GetBytes(record.Str);

            m_DataFile.Write(BitConverter.GetBytes(strBytes.Length), 0, 4);
            m_DataFile.Write(strBytes, 0, strBytes.Length);

            record.ClearStr();
        }

        public void WriteTempFileRecord(TempFileRecord record)
        {
            m_ProactiveTaskRunner.WaitForProactiveTask();
            m_ProactiveTaskRunner.StartProactiveTask(() => WriteTempFileRecordImpl(record));
        }

        private void WriteTempFileRecordImpl(TempFileRecord record)
        {
            m_DataFile.Write(BitConverter.GetBytes(record.Number), 0, 8);
            m_DataFile.Write(BitConverter.GetBytes(record.StrAsByteArray.Length), 0, 4);
            m_DataFile.Write(record.StrAsByteArray, 0, record.StrAsByteArray.Length);

            record.ClearStr();
        }

        public void SwitchToReadMode()
        {
            m_ProactiveTaskRunner.WaitForProactiveTask();
            SwitchToReadModeImpl();

            m_ProactiveTaskRunner.StartProactiveTask<TempFileRecord?>(() => ReadRecordToMergeImpl());
        }

        private void SwitchToReadModeImpl()
        {
            if (m_ReadMode)
            {
                throw new InvalidOperationException("Unexpected internal error! 'TempFile.SwitchToReadMode' was called in read mode.");
            }

            FlushDataAndDisposeFilesImpl();

            m_DataFile = new FileStream(
                m_DataFilePah,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                Constants.FILE_BUFFER_SIZE,
                FileOptions.SequentialScan | FileOptions.DeleteOnClose);

            m_ReadMode = true;
        }

        public TempFileRecord? ReadRecordToMerge()
        {
            var result = m_ProactiveTaskRunner.WaitForProactiveTask<TempFileRecord?>();

            if (result.HasValue)
            {
                m_ProactiveTaskRunner.StartProactiveTask<TempFileRecord?>(() => ReadRecordToMergeImpl());
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
            byte[] bufferNumber = new byte[8];
            var bytesRead = m_DataFile.Read(bufferNumber, 0, 8);

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
            bytesRead = m_DataFile.Read(bufferStringLength, 0, 4);
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
            bytesRead = m_DataFile.Read(bufferString, 0, stringLength);
            if (bytesRead != stringLength)
            {
                throw new InvalidOperationException("Unexpected internal error! Can't read String for the next record of temporary segmented file.");
            }

            return new TempFileRecord(BitConverter.ToUInt64(bufferNumber, 0), bufferString);
        }

        public void FlushDataAndDisposeFiles()
        {
            m_ProactiveTaskRunner.WaitForProactiveTask();
            FlushDataAndDisposeFilesImpl();
        }

        private  void FlushDataAndDisposeFilesImpl()
        {
            if (m_DataFile != null)
            {
                m_DataFile.Flush();
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
            if (!m_DisposedValue)
            {
                if (disposing)
                {
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
