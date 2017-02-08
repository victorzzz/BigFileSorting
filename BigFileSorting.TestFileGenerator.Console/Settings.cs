using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CmdLine;

namespace BigFileSorting.TestFileGenerator.Console
{
    [CommandLineArguments(
        Program = "Test file generator for big file sorting", 
        Title = "Command line tool to generate big file with simple structure: <number.<string>>")]
    public class Settings
    {
        [CommandLineParameter(
            Name = "TargetFilePath",
            Description = "Path to the target file",
            Command = "target_file",
            Required = true)]
        public string TargetFilePath { get; set; }

        [CommandLineParameter(
            Name = "Encoding",
            Description = "Encoding name",
            Command = "encoding",
            Default = "unicode")]
        public string EncodingName { get; set; }

        [CommandLineParameter(
            Name = "AproximateFileSize",
            Description = "Approximate file size in Gb",
            Command = "file_size_gb",
            Default = 10)]
        public int AproximateFileSize { get; set; }

        [CommandLineParameter(
            Name = "NumberOfUniqueSrings",
            Description = "Number of unique strings",
            Command = "unique_strings",
            Default = 100000)]
        public int NumberOfUniqueSrings { get; set; }

    }
}
