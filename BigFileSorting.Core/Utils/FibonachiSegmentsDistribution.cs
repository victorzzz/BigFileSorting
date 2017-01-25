using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigFileSorting.Core.Utils
{
    internal class FibonachiSegmentsDistribution
    {
        private int[] m_Fibonachi = { 1, 1 };
        private int[] m_ActualSegments = { 0, 0 };
        private int m_TempFileIndexToWriteSegment = 0;

        public int NextTempFileIndex(int numberOfSegment)
        {
            if (numberOfSegment != m_ActualSegments.Sum() + 1)
            {
                throw new InvalidOperationException();
            }

            while (true)
            {
                int result;
                for (int i = 0; i < 2; ++i)
                {
                    if (m_ActualSegments[m_TempFileIndexToWriteSegment] < m_Fibonachi[m_TempFileIndexToWriteSegment])
                    {
                        ++m_ActualSegments[m_TempFileIndexToWriteSegment];
                        result = m_TempFileIndexToWriteSegment;
                        m_TempFileIndexToWriteSegment = 1 - m_TempFileIndexToWriteSegment;

                        return result;
                    }
                    else
                    {
                        m_TempFileIndexToWriteSegment = 1 - m_TempFileIndexToWriteSegment;
                    }
                }

                int newFibonachi = m_Fibonachi.Sum();
                m_Fibonachi[0] = m_Fibonachi[1];
                m_Fibonachi[1] = newFibonachi;
            }
        }

    }
}
