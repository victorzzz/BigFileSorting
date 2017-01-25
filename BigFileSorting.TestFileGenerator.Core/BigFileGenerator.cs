﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace BigFileSorting.TestFileGenerator.Core
{
    public static class BigFileGenerator
    {
        private const int WRITE_FILE_BUFFER = 1024 * 1024 * 4;
        private const int NUMBER_OF_UNIQUE_STRINGS = 10;

        public static async Task Generate(string filePath, long size, Encoding encoding)
        {
            var random = new Random(DateTime.Now.Millisecond);

            var strings = new List<string>(capacity: NUMBER_OF_UNIQUE_STRINGS);
            for(int i = 0; i < NUMBER_OF_UNIQUE_STRINGS; ++i)
            {
                var k = random.Next(1, 5);
                var builder = new StringBuilder(capacity: 40 * k);
                for (int j = 0; j < k; ++j)
                {
                    builder.Append(Guid.NewGuid().ToString());
                }

                strings.Add(builder.ToString());
            }

            long wrtittenSize = 0;

            using (var fileStream = new FileStream(filePath, FileMode.CreateNew, FileAccess.Write, FileShare.None, WRITE_FILE_BUFFER, FileOptions.Asynchronous))
            using (var streamWriter = new StreamWriter(fileStream, encoding))
            {
                while (wrtittenSize < size)
                {
                    var number = Math.BigMul(random.Next(0, 1000), random.Next(0, 1000));
                    var k = random.Next(0, NUMBER_OF_UNIQUE_STRINGS - 1);
                    string str = strings[k];

                    string lineToFile = $"{number}.{str}";
                    wrtittenSize += lineToFile.Length * 2;

                    await streamWriter.WriteLineAsync(lineToFile).ConfigureAwait(false);
                }
            }
        }
    }
}