using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using BigFileSorting.Core;
using BigFileSorting.TestFileGenerator.Core;

namespace BigFileSorting.Test
{
    [TestClass]
    public class BigFileSorterTest
    {
        private const string BIG_SOURCE_FILE_PATH = @"f:\BigFileSorterTestData\TestFile.txt";
        private const long TEST_FILE_SIZE = 1024L * 1024L * 1024L * 5L; //5 GB

        [TestInitialize]
        public void TestInitialize()
        {
            if(!File.Exists(BIG_SOURCE_FILE_PATH))
            {
                BigFileGenerator.Generate(BIG_SOURCE_FILE_PATH, TEST_FILE_SIZE, Encoding.Unicode).Wait();
            }
        }

        [TestMethod]
        public async Task Sort()
        {
            var r = await BigFileSorter.Sort(
                BIG_SOURCE_FILE_PATH,
                @"d:\testtemp",
                new List<string>() { @"d:\testtemp" },
                Encoding.Unicode, 
                CancellationToken.None).ConfigureAwait(false);

            Assert.AreEqual(r, new Tuple<long, long, long>(0,0,0));
        }
    }
}
