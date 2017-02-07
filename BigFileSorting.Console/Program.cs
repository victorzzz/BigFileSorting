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

namespace BigFileSorting.Console
{
    class Program
    {
        [DllImport("Kernel32")]
        public static extern bool SetConsoleCtrlHandler(HandlerRoutine Handler, bool add);
        public delegate bool HandlerRoutine(CtrlTypes CtrlType);
        public enum CtrlTypes { CTRL_C_EVENT = 0, CTRL_BREAK_EVENT, CTRL_CLOSE_EVENT, CTRL_LOGOFF_EVENT = 5, CTRL_SHUTDOWN_EVENT }

        static void Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += (s, e) => WriteToConsole("UnhandledException", e.ExceptionObject.ToString(), ConsoleColor.Red);

            try
            {
                WriteToConsole("Big file sorter", "Starting...");

                var cts = new CancellationTokenSource();
                SetConsoleCtrlHandler(new HandlerRoutine(ctrlType => 
                {
                    cts.Cancel();
                    return true;
                }), true);

                var settings = CommandLine.Parse<Settings>();
                var encoding = Encoding.GetEncoding(settings.EncodingName);
                var sorter = new BigFileSorterNew(cts.Token, encoding);

                sorter.Sort(settings.SourceFilePath, settings.TargetFilePath, settings.TempFolders, 0);

                WriteToConsole("Big file sorter", "Finished.");
            }
            catch (AggregateException e)
            {
                foreach (var sub in e.InnerExceptions)
                {
                    WriteToConsole("Exception", sub.Message, ConsoleColor.Red);
                }
                WriteToConsole("Big file sorter", "Finished with error.");
                Environment.Exit(1); // 
            }
            catch (Exception e)
            {
                WriteToConsole("Exception", e.Message, ConsoleColor.Red);
                WriteToConsole("Big file sorter", "Finished with error.");
                Environment.Exit(1); // 
            }
        }

        private static void WriteToConsole(string operation, string message, ConsoleColor color = ConsoleColor.White)
        {
            System.Console.ForegroundColor = color;
            System.Console.WriteLine("{0} | {1} | {2}",
                DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"), 
                operation, message);
        }
    }
}
