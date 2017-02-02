using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace BigFileSorting.Core
{
    internal class TotalAllocatedMemoryLimiter
    {
        private long m_Balans;
        private volatile bool m_On;
        private readonly AutoResetEvent m_Event = new AutoResetEvent(initialState: false);
        private readonly ReaderWriterLockSlim m_BalansLock = new ReaderWriterLockSlim();

        private readonly CancellationToken m_CancellationToken;

        public TotalAllocatedMemoryLimiter(bool on, CancellationToken cancellationToken)
        {
            m_CancellationToken = cancellationToken;
            m_On = on;
        }

        public void ResetBalans()
        {
            m_BalansLock.EnterWriteLock();
            try
            {
                m_Balans = 0;
            }
            finally
            {
                m_BalansLock.ExitWriteLock();
            }

            TurnOn();
        }

        public void TurnOff()
        {
            m_On = false;
            m_Event.Set();
        }

        public void TurnOn()
        {
            m_On = true;
            m_Event.Set();
        }

        public void MemoryDisposed(long size)
        {
            m_BalansLock.EnterWriteLock();
            try
            {
                m_Balans -= size;
            }
            finally
            {
                m_BalansLock.ExitWriteLock();
            }

            m_Event.Set();
        }

        public void MemoryAllocated(long size)
        {
            m_BalansLock.EnterWriteLock();
            try
            {
                m_Balans += size;
            }
            finally
            {
                m_BalansLock.ExitWriteLock();
            }
        }

        public void WaitForPosibilityToAllocMemory()
        {
            while (true)
            {
                m_CancellationToken.ThrowIfCancellationRequested();

                if (!m_On)
                {
                    return;
                }

                m_Event.WaitOne();

                m_CancellationToken.ThrowIfCancellationRequested();

                if (!m_On)
                {
                    return;
                }

                m_BalansLock.EnterReadLock();
                try
                {
                    if (m_Balans < 0)
                    {
                        return;
                    }
                }
                finally
                {
                    m_BalansLock.ExitReadLock();
                }

                Task.Delay(TimeSpan.FromSeconds(Constants.MEMORY_LIMITER_DELAY_SECONDS));
            }
        }
    }
}
