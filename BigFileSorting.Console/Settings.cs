using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CmdLine;

namespace BigFileSorting.Console
{
    [CommandLineArguments(Program = "Big file sorter", Title = "Big file sorter")]
    public class Settings
    {
        [CommandLineParameter(
            Name = "SourceFilePath",
            Description = "Path to the source file",
            Command = "source_file")]
        public string SourceFilePath { get; set; }

        [CommandLineParameter(
            Name = "TargetFilePath",
            Description = "Path to the target file",
            Command = "target_file")]
        public string TargetFilePath { get; set; }

        [CommandLineParameter(
            Name = "tempFolder",
            Description = "Path to the temp folder",
            Command = "temp_folder")]
        public List<string> TempFolders { get; set; }

        [CommandLineParameter(
            Name = "encoding",
            Description = "Encoding name", 
            Command = "encoding")]
        public string EncodingName { get; set; }
    }
}
