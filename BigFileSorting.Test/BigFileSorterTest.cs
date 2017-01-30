using System;
using System.Collections.Generic;
using System.Diagnostics;
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
        private const string BIG_SOURCE_FILE_PATH = @"e:\BigFileSorterTestData\TestFile.txt";
        private const string BIG_DESTINATION_FILE_PATH = @"e:\BigFileSorterTestData\TestFile.target.txt";
        private const long TEST_FILE_SIZE = 1024L * 1024L * 1024L * 10L;
        private const long TEST_MEMORY_USE = -1;

        [TestInitialize]
        public void TestInitialize()
        {
            if(!File.Exists(BIG_SOURCE_FILE_PATH))
            {
                Trace.WriteLine("Generating new file!");
                BigFileGenerator.Generate(BIG_SOURCE_FILE_PATH, TEST_FILE_SIZE, Encoding.Unicode).Wait();
                Trace.WriteLine("Generating done!");

               
                GC.Collect(2, GCCollectionMode.Forced, true, true);
            }
        }

        [TestMethod]
        public async Task Sort()
        {
            Trace.WriteLine("Hi!");

            var sw = Stopwatch.StartNew();

            await BigFileSorterNew.Sort(
                BIG_SOURCE_FILE_PATH,
                BIG_DESTINATION_FILE_PATH,
                new List<string>() { @"c:\testtemp" },
                Encoding.Unicode,
                TEST_MEMORY_USE,
                CancellationToken.None).ConfigureAwait(false);

            sw.Stop();

            Trace.WriteLine($"TotalTime: {sw.Elapsed}");
        }
    }
}
