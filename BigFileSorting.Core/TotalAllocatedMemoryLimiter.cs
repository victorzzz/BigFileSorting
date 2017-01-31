using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using Nito.AsyncEx;

namespace BigFileSorting.Core
{
    internal class TotalAllocatedMemoryLimiter
    {
        private long m_Limit;
        private volatile bool m_On;
        private readonly AsyncAutoResetEvent m_Event = new AsyncAutoResetEvent(set: false);
        private readonly AsyncReaderWriterLock m_LimitLock = new AsyncReaderWriterLock();

        private readonly CancellationToken m_CancellationToken;

        public TotalAllocatedMemoryLimiter(bool on, CancellationToken cancellationToken)
        {
            m_CancellationToken = cancellationToken;
            m_Limit = GC.GetTotalMemory(true);
            m_On = on;
        }

        public async Task ResetLimit()
        {
            using (var lockObj = await m_LimitLock.WriterLockAsync(m_CancellationToken).ConfigureAwait(false))
            {
                m_Limit = GC.GetTotalMemory(true);
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

        public void NotifyMemoryChanged()
        {
            m_Event.Set();
        }

        public async Task WaitForPosibilityToAllocMemory()
        {
            while (true)
            {
                if (!m_On)
                {
                    return;
                }

                await m_Event.WaitAsync(m_CancellationToken).ConfigureAwait(false);

                if (!m_On)
                {
                    return;
                }

                var currentTotalMemory = GC.GetTotalMemory(forceFullCollection: true);

                using (var lockObj = await m_LimitLock.ReaderLockAsync(m_CancellationToken).ConfigureAwait(false))
                {
                    if (currentTotalMemory < m_Limit)
                    {
                        return;
                    }
                }

                await Task.Delay(TimeSpan.FromSeconds(Constants.MEMPRY_LIMITER_DELAY_SECONDS)).ConfigureAwait(false);
            }
        }
    }
}
