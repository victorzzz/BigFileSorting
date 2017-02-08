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
        private const string BIG_SOURCE_FILE_PATH = @"f:\BigFileSorterTestData\TestFile.txt";
        private const string BIG_DESTINATION_FILE_PATH = @"f:\BigFileSorterTestData\TestFile.target.txt";
        private const long TEST_FILE_SIZE = 1024L * 1024L * 500L;
        private const long TEST_MEMORY_USE = 1024L * 1024L * 100L;
        private const int NUMBER_OF_UNOIQUE_STRINGS_IN_TEST_FILE = 500;

        private Dictionary<FileRecord, long> m_CheckCorrectnessDictionary;

        [TestInitialize]
        public void TestInitialize()
        {
            m_CheckCorrectnessDictionary = new Dictionary<FileRecord, long>(new FileRecordEualityComparer());

            if (!File.Exists(BIG_SOURCE_FILE_PATH))
            {
                Trace.WriteLine("Generating new file!");

                BigFileGenerator.Generate(
                    BIG_SOURCE_FILE_PATH,
                    TEST_FILE_SIZE,
                    Encoding.Unicode,
                    m_CheckCorrectnessDictionary, NUMBER_OF_UNOIQUE_STRINGS_IN_TEST_FILE);

                Trace.WriteLine("Generating done!");

                GC.Collect(2, GCCollectionMode.Forced, true, true);
            }
        }

        [TestMethod]
        public void Sort()
        {
            Trace.WriteLine("Hi!");


            // ACT
            var sw = Stopwatch.StartNew();

            var sorter = new BigFileSorterNew(CancellationToken.None, Encoding.Unicode);

            sorter.Sort(
                BIG_SOURCE_FILE_PATH,
                BIG_DESTINATION_FILE_PATH,
                new List<string>() { @"c:\testtemp" },
                TEST_MEMORY_USE);

            sw.Stop();

            Trace.WriteLine($"TotalTime: {sw.Elapsed}");

            // ASSERT
            // check correctness - it should be sorted and contains expected number of item

            FileRecord? previousRecord = null;
            var destinationCheckDictionary = new Dictionary<FileRecord, long>(new FileRecordEualityComparer());

            using (var fileReader = new BigFileReader(BIG_DESTINATION_FILE_PATH, Encoding.Unicode, CancellationToken.None))
            {
                while (true)
                {
                    var record = fileReader.ReadRecord();

                    if (!record.HasValue)
                    {
                        break;
                    }

                    if (previousRecord.HasValue)
                    {
                        Assert.IsTrue(previousRecord.Value.CompareTo(record.Value) <= 0);
                    }

                    previousRecord = record;
                    destinationCheckDictionary.IncrementDictionaryValue<FileRecord>(record.Value, 1);
                }
            }

            Assert.IsTrue(m_CheckCorrectnessDictionary.DictionaryEqual(destinationCheckDictionary));
        }
    }
}
