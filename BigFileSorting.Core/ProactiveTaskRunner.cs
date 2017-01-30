using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace BigFileSorting.Core
{
    internal class ProactiveTaskRunner
    {
        private Task m_ProactiveTask;
        private CancellationToken m_CancellationToken;

        public ProactiveTaskRunner(CancellationToken cancellationToken)
        {
            m_CancellationToken = cancellationToken;
        }

        public async Task<ResultType> WaitForProactiveTaskAsync<ResultType>()
        {
            if (m_ProactiveTask == null)
            {
                throw new InvalidOperationException("Unexpected internal error! WaitForProactiveTaskAsync<ResultType> was called when m_ProactiveTask is null");
            }

            var task = m_ProactiveTask as Task<ResultType>;
            if (task == null)
            {
                throw new InvalidOperationException("Unexpected internal error! WaitForProactiveTaskAsync<ResultType> was called when m_ProactiveTask is Task");
            }
            var result = await task.ConfigureAwait(false);

            m_ProactiveTask = null;

            return result;
        }

        public async Task WaitForProactiveTaskAsync()
        {
            if (m_ProactiveTask != null)
            {
                await m_ProactiveTask.ConfigureAwait(false);
                m_ProactiveTask = null;
            }
        }

        public void StartProactiveTask(Func<Task> func)
        {
            if (m_ProactiveTask != null)
            {
                throw new InvalidOperationException("Unexpected internal error! 'TempFile.StartProactiveTask' was called when another proactive task is in progress...");
            }

            m_ProactiveTask = Task.Run(func, m_CancellationToken);
        }

        public void StartProactiveTask<ResultType>(Func<Task<ResultType>> func)
        {
            if (m_ProactiveTask != null)
            {
                throw new InvalidOperationException("Unexpected internal error! 'TempFile.StartProactiveTask' was called when another proactive task is in progress...");
            }

            m_ProactiveTask = Task.Run(func, m_CancellationToken);
        }
    }
}
