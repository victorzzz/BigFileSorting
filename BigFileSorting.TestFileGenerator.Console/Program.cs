using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BigFileSorting.Console.Common;
using BigFileSorting.TestFileGenerator.Core;
using CmdLine;

namespace BigFileSorting.TestFileGenerator.Console
{
    class Program
    {
        static void Main(string[] args)
        {
            ConsoleHelper.ExecuteCommandLineTool("Test big file generator", cancelationToken =>
            {
                var settings = CommandLine.Parse<Settings>();
                var encoding = Encoding.GetEncoding(settings.EncodingName);

                if (File.Exists(settings.TargetFilePath))
                {
                    throw new InvalidOperationException($"File '{settings.TargetFilePath}' already exists.");
                }

                if (settings.AproximateFileSize <= 0)
                {
                    throw new InvalidOperationException("AproximateFileSize should be > 0.");
                }

                if (settings.NumberOfUniqueSrings <= 0)
                {
                    throw new InvalidOperationException("NumberOfUniqueSrings should be > 0.");
                }

                BigFileGenerator.Generate(settings.TargetFilePath, 
                    settings.AproximateFileSize * 1024L * 1024L * 1024L, encoding, null, settings.NumberOfUniqueSrings);
            });
        }
    }
}
