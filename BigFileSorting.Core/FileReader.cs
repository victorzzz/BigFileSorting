using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text;
using System.Numerics;
using System.Threading.Tasks;
using System.Reflection;
using BigFileSorting.Core.Exceptions;
using System.Threading;
using Nito.AsyncEx;

namespace BigFileSorting.Core
{
    internal class FileReader<TNumber> : IDisposable
    {
        internal struct FileReadingBuffer
        {
            public byte[] m_Buffer;
            public int m_BytesInBuffer;
        }

        private readonly FileStream m_FileStream;
        private readonly int m_BufferSize;
        private readonly CancellationToken m_CancellationToken;

        private readonly List<FileReadingBuffer> m_Buffers = new List<FileReadingBuffer>(Constants.NUMBER_OF_PROGRESSIVE_READING_BUFFERS);
        private int m_ActiveBufferIndex;
        private int m_ProgressiveReadingBufferIndex;

        private readonly AsyncReaderWriterLock m_IndexesLock = new AsyncReaderWriterLock();
        private AsyncManualResetEvent m_ToFileReaderEvent = new AsyncManualResetEvent(false);
        private AsyncManualResetEvent m_ToProcessorEvent = new AsyncManualResetEvent(false);

        private Task m_ProgressiveReadingTask;
        private int m_BufferReadPosition;

        private bool disposedValue = false; // To detect redundant calls for 'Dispose'

        public FileReader(string filePath, int bufferSize, CancellationToken cancellationToken)
        {
            if (bufferSize < Constants.MIN_FILE_READING_BUFFER_SIZE)
            {
                throw new ArgumentOutOfRangeException(nameof(bufferSize));
            }

            m_CancellationToken = cancellationToken;
            m_BufferSize = bufferSize;

            for(int i = 0; i < Constants.NUMBER_OF_PROGRESSIVE_READING_BUFFERS; ++i)
            {
                m_Buffers.Add(new FileReadingBuffer()
                {
                    m_Buffer = new byte[m_BufferSize],
                    m_BytesInBuffer = 0
                });
            }

            m_FileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize, useAsync: true);

            StartProgressiveReadingTask();

            m_ToFileReaderEvent.Set();
        }

        public List<FileRecord<TNumber>> ReadSegment(long maxSourceSize)
        {
            long resultSize = 0;

            List<FileRecord<TNumber>> result = new List<FileRecord<TNumber>>();

            CanContinueProcessing().Wait();

            long readBytes;
            var record = ReadRecord(out readBytes);

            while (true)
            {
                m_CancellationToken.ThrowIfCancellationRequested();

                if (record == null)
                {
                    return result;
                }
                else
                {
                    result.Add(record);
                    resultSize += readBytes;
                    if (resultSize > maxSourceSize)
                        return result;
                }
            }
        }

        #region Private Methods

        private bool ReadRecord(ref FileRecord<TNumber> fileRecord)
        {
            TNumber number;

            if(!ReadNumber(out number))
            {
                return false;
            }

            var str = ReadLogicalStringAsByteArray();

            fileRecord = new  FileRecord<TNumber>(number, str);
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="number"></param>
        /// <exception cref="InvalidFileException">Can't read number</exception> 
        /// <returns>false - end of file</returns>
        private bool ReadNumber(out TNumber number)
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private async Task<byte[]> ReadLogicalStringAsByteArray()
        {
            List<byte> result = new List<byte>();
            while (true)
            {
                var b = await GetNextByte();
                if (!b.HasValue)
                {
                    break;
                }

                if (b.Value == 0x0A || b.Value == 0x0D)

                result.Add(b.Value);
            }

            return result.ToArray();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="nextByte"></param>
        /// <returns>false - end of file</returns>
        private async Task<byte?> GetNextByte()
        {
            byte nextByte = 0;
            if (GetNextByteFromBuffer(ref nextByte))
            {
                return nextByte;
            }

            await NextActiveBufferIndex();
            await CanContinueProcessing();

            if (GetNextByteFromBuffer(ref nextByte))
            {
                return nextByte;
            }
            else
            {
                return null;
            }
        }

        private void StartProgressiveReadingTask()
        {
            m_ProgressiveReadingTask = Task.Run(
                async () =>
                {
                    while (true)
                    {
                        m_CancellationToken.ThrowIfCancellationRequested();

                        await CanContinueReading();

                        FileReadingBuffer buffer = m_Buffers[m_ProgressiveReadingBufferIndex];
                        buffer.m_BytesInBuffer = await m_FileStream.ReadAsync(buffer.m_Buffer, 0, m_BufferSize, m_CancellationToken);

                        await NextProgressiveReadingBufferIndex();

                        if (buffer.m_BytesInBuffer == 0)
                        {
                            break;
                        }
                    }
                },
                m_CancellationToken);
        }

        private async Task CanContinueProcessing()
        {
            using (var readLock = await m_IndexesLock.ReaderLockAsync(m_CancellationToken))
            {
                if (m_ActiveBufferIndex == m_ProgressiveReadingBufferIndex)
                {
                    await m_ToProcessorEvent.WaitAsync();
                    m_ToProcessorEvent.Reset();
                }
            }
        }

        private async Task CanContinueReading()
        {
            using (var readLock = await m_IndexesLock.ReaderLockAsync(m_CancellationToken))
            {
                if (m_ActiveBufferIndex == m_ProgressiveReadingBufferIndex)
                {
                    await m_ToFileReaderEvent.WaitAsync();
                    m_ToFileReaderEvent.Reset();
                }
            }
        }


        private static void NextBufferIndex(ref int bufferIndex)
        {
            if (bufferIndex == Constants.NUMBER_OF_PROGRESSIVE_READING_BUFFERS - 1)
            {
                bufferIndex = 0;
            }
            else
            {
                ++bufferIndex;
            }
        }

        private async Task NextActiveBufferIndex()
        {
            using (var writeLock = await m_IndexesLock.WriterLockAsync(m_CancellationToken))
            {
                NextBufferIndex(ref m_ActiveBufferIndex);

                m_ToFileReaderEvent.Set();
            }
        }

        private async Task NextProgressiveReadingBufferIndex()
        {
            using (var writeLock = await m_IndexesLock.WriterLockAsync(m_CancellationToken))
            {
                NextBufferIndex(ref m_ProgressiveReadingBufferIndex);

                m_ToProcessorEvent.Set();
            }
        }

        private bool GetNextByteFromBuffer(ref byte nextByte)
        {
            FileReadingBuffer buffer = m_Buffers[m_ActiveBufferIndex];
            if (m_BufferReadPosition < buffer.m_BytesInBuffer)
            {
                nextByte = buffer.m_Buffer[m_BufferReadPosition];
                ++m_BufferReadPosition;
                return true;
            }
            else
            {
                return false;
            }
        }

        #endregion

        #region IDisposable Support

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    m_FileStream.Dispose();
                }

                disposedValue = true;
            }
        }

        // This code added to correctly implement the disposable pattern.
        void IDisposable.Dispose()
        {
            Dispose(true);
        }

        #endregion
    }
}
