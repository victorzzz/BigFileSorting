using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using BigFileSorting.Core.Utils;

namespace BigFileSorting.Test
{
    [TestClass]
    public class FibonachiSegmentsDistributionTests
    {
        private FibonachiSegmentsDistribution testInstance;
        private static int[] ExpectedIndexes = { 0,1,1,0,1,0,1,1,0,1,0,1,1,0,1,0,1,0,1,1,1,
0,1,0,1,0,1,0,1,0,1,1,1,1,0,1,0,1,0,1,0,1,0,1,0,1,0,1,0,1,1,1,1,1,1,0};

        [TestInitialize]
        public void TestInitialize()
        {
            testInstance = new FibonachiSegmentsDistribution();
        }

        [TestMethod]
        public void DistributeSegments()
        {
            for(int i = 1; i<56; ++i)
            {
                var r = testInstance.NextTempFileIndex(i);
                Assert.AreEqual(ExpectedIndexes[i-1], r);
            }
        }
    }
}
