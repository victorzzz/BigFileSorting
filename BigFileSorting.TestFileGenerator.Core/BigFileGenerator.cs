using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using BigFileSorting.Core;

namespace BigFileSorting.TestFileGenerator.Core
{
    public static class BigFileGenerator
    {
        public static void Generate(
            string filePath,
            long size,
            Encoding encoding,
            Dictionary<FileRecord, long> dictionaryToCheck,
            int numOfUniqueStrings)
        {
            var random = new Random(DateTime.Now.Millisecond);

            var strings = new List<string>(capacity: numOfUniqueStrings);
            for(int i = 0; i < numOfUniqueStrings; ++i)
            {
                var k = random.Next(0, 5);
                var builder = new StringBuilder(capacity: 40 * k);
                for (int j = 0; j < k; ++j)
                {
                    builder.Append(Guid.NewGuid().ToString());
                }

                strings.Add(builder.ToString());
            }

            long wrtittenSize = 0;

            using (var fileStream = new FileStream(filePath, FileMode.CreateNew, FileAccess.Write, FileShare.None, Constants.FILE_BUFFER_SIZE, FileOptions.None))
            using (var streamWriter = new StreamWriter(fileStream, encoding, Constants.FILE_BUFFER_SIZE))
            {
                while (wrtittenSize < size)
                {
                    var number = Math.BigMul(random.Next(0, 1000), random.Next(0, 1000));
                    var k = random.Next(0, numOfUniqueStrings - 1);
                    string str = strings[k];

                    string lineToFile = $"{number}.{str}";
                    wrtittenSize += lineToFile.Length * 2;

                    streamWriter.WriteLine(lineToFile);

                    var fileRecord = new FileRecord((ulong)number, str);
                    dictionaryToCheck?.IncrementDictionaryValue<FileRecord>(fileRecord, 1);
                }
            }
        }
    }
}
