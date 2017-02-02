using System;
using System.Runtime.InteropServices;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace BigFileSorting.Core
{
    internal class ConcurrentSorter : IDisposable
    {
        private readonly ReaderWriterLockSlim m_ListLock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
        private readonly int m_ListCapasityLimit;
        private readonly long m_SegmentSizeLimit;
        private readonly long m_PresortingSegmentSizeLimit;
        private readonly int m_PresortingListCapacity;
        private readonly CancellationToken m_CancellationToken;

        private readonly BlockingCollection<List<FileRecord>> m_PreSortingQueue;
        private readonly List<Task> m_PresortingTasks;

        private List<FileRecord> m_List;
        private List<FileRecord> m_PresortingList;
        private long m_CurrentSegmentSize;
        private long m_CurrentSegmentItemsNumber;
        private long m_CurrentPresortingSegmentSize;

        public ConcurrentSorter(long memoryToUse, int coresToUse, CancellationToken cancellationToken)
        {
            if (coresToUse < 2)
            {
                throw new InvalidOperationException("ConcurrentSorter: coresToUse < 2");
            }

            var listStructSize = Marshal.SizeOf(typeof(FileRecord));

            m_CancellationToken = cancellationToken;
            var memoryForData = memoryToUse / 4 * 3;
            m_PresortingSegmentSizeLimit = memoryForData / 100;
            m_PresortingListCapacity = (int) (m_PresortingSegmentSizeLimit / (Constants.APROXIMATE_RECORD_SIZE + listStructSize));

            m_ListCapasityLimit = Math.Min(
                (int)(memoryForData / (Constants.APROXIMATE_RECORD_SIZE + listStructSize)),
                Constants.MAX_ARRAY_BYTES / listStructSize);

            m_SegmentSizeLimit = memoryForData
                - m_ListCapasityLimit * listStructSize
                - coresToUse * 2 * (m_PresortingSegmentSizeLimit + m_PresortingListCapacity * listStructSize);

            m_List = new List<FileRecord>(capacity: m_ListCapasityLimit);

            m_PreSortingQueue = new BlockingCollection<List<FileRecord>>(coresToUse * 2);
            m_PresortingTasks = new List<Task>(coresToUse);

            for (int i = 0; i < coresToUse; ++i)
            {
                m_PresortingTasks.Add(
                    Task.Factory.StartNew(
                        PreSortAndAddToList,
                        m_CancellationToken,
                        TaskCreationOptions.LongRunning,
                        TaskScheduler.Default));
            }

            NewPresortingSegment();
        }

        public List<FileRecord> ReadRecordsAndSort(BigFileReader fileReader)
        {
            while(true)
            {
                m_CancellationToken.ThrowIfCancellationRequested();

                var record = fileReader.ReadRecord();
                if (!record.HasValue)
                {
                    break;
                }

                m_CancellationToken.ThrowIfCancellationRequested();

                if (!AddRecord(record.Value))
                {
                    break;
                }
            }

            if (m_PresortingList != null && m_PresortingList.Count > 0)
            {
                PreSortAsynchronously();
                m_PresortingList = null;
            }

            WaitForPresorting();

            m_CancellationToken.ThrowIfCancellationRequested();

            m_List.Sort();

            return m_List;
        }

        private void WaitForPresorting()
        {
            if (m_PreSortingQueue != null && !m_PreSortingQueue.IsAddingCompleted)
            {
                m_PreSortingQueue.CompleteAdding();
            }

            Task.WhenAll(m_PresortingTasks).GetAwaiter().GetResult();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="record"></param>
        /// <returns>true - can be added more recors, false - limit exceded</returns>
        private bool AddRecord(FileRecord record)
        {
            if (m_PresortingList == null)
            {
                throw new InvalidOperationException("ConcurrentSorter: limit exceded!");
            }

            m_PresortingList.Add(record);
            m_CurrentPresortingSegmentSize += RecordSize(record);

            if (m_PresortingList.Count >= m_PresortingListCapacity
                || m_CurrentPresortingSegmentSize > m_PresortingSegmentSizeLimit)
            {
                PreSortAsynchronously();

                m_CurrentSegmentSize += m_CurrentPresortingSegmentSize;
                m_CurrentSegmentItemsNumber += m_PresortingList.Count;

                if (m_CurrentSegmentSize > m_SegmentSizeLimit
                    || m_CurrentSegmentItemsNumber >= m_ListCapasityLimit)
                {
                    m_PresortingList = null;
                    return false;
                }
                else
                {
                    NewPresortingSegment();
                }
            }

            return true;
        }

        private void NewPresortingSegment()
        {
            m_PresortingList = new List<FileRecord>(capacity: m_PresortingListCapacity);
            m_CurrentPresortingSegmentSize = 0;
        }

        private int RecordSize(FileRecord record)
        {
            return record.Str.Length * 2;
        }

        private void PreSortAndAddToList()
        {
            while(true)
            {
                m_CancellationToken.ThrowIfCancellationRequested();

                if (m_PreSortingQueue.IsCompleted)
                {
                    break;
                }

                List<FileRecord> listToSort;
                try
                {
                    listToSort = m_PreSortingQueue.Take(m_CancellationToken);
                }
                catch(InvalidOperationException)
                {
                    break;
                }

                m_CancellationToken.ThrowIfCancellationRequested();

                listToSort.Sort();

                m_ListLock.EnterWriteLock();
                try
                {
                    m_List.AddRange(listToSort);
                }
                finally
                {
                    m_ListLock.ExitWriteLock();
                }
            }
        }

        private void PreSortAsynchronously()
        {
            m_PreSortingQueue.Add(m_PresortingList);
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    WaitForPresorting();

                    m_ListLock.Dispose();

                    m_List = null;
                    m_PresortingList = null;
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
