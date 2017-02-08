using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using CmdLine;
using System.Runtime.InteropServices;
using System.Reflection;
using BigFileSorting.Core;
using BigFileSorting.Console.Common;
using System.IO;

namespace BigFileSorting.Console
{
    class Program
    {
        static void Main(string[] args)
        {
            ConsoleHelper.ExecuteCommandLineTool("Big file sorter", cancelationToken =>
            {
                var settings = CommandLine.Parse<Settings>();
                var encoding = Encoding.GetEncoding(settings.EncodingName);

                if (!File.Exists(settings.SourceFilePath))
                {
                    throw new InvalidOperationException("Source file was not found");
                }

                if (File.Exists(settings.TargetFilePath))
                {
                    throw new InvalidOperationException($"File {settings.TargetFilePath} already exists.");
                }

                var sorter = new BigFileSorterNew(cancelationToken, encoding);
                sorter.Sort(settings.SourceFilePath, settings.TargetFilePath, settings.TempFolders, 0);
            });
        }
    }
}
