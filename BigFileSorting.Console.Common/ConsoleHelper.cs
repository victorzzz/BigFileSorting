using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using System.Reflection;
using System.Threading;

namespace BigFileSorting.Console.Common
{
    public static class ConsoleHelper
    {
        [DllImport("Kernel32")]
        public static extern bool SetConsoleCtrlHandler(HandlerRoutine handler, bool add);
        public delegate bool HandlerRoutine(CtrlTypes ctrlType);
        public enum CtrlTypes
        {
            CTRL_C_EVENT = 0,
            CTRL_BREAK_EVENT,
            CTRL_CLOSE_EVENT,
            CTRL_LOGOFF_EVENT = 5,
            CTRL_SHUTDOWN_EVENT
        }

        public static void WriteToConsole(string operation, string message, ConsoleColor color = ConsoleColor.White)
        {
            System.Console.ForegroundColor = color;
            System.Console.WriteLine("{0:dd.MM.yyyy HH:mm:ss} | {1} | {2}", DateTime.Now,
                operation, message);
        }

        public static void ExecuteCommandLineTool(string appName, Action<CancellationToken> action)
        {
            AppDomain.CurrentDomain.UnhandledException += (s, e) => 
                WriteToConsole("UnhandledException", e.ExceptionObject.ToString(), ConsoleColor.Red);

            try
            {
                WriteToConsole(appName, "Starting...");

                var cts = new CancellationTokenSource();
                SetConsoleCtrlHandler(new HandlerRoutine(ctrlType =>
                {
                    cts.Cancel();
                    return true;
                }), true);

                action(cts.Token);

                WriteToConsole(appName, "Finished.");
            }
            catch (AggregateException e)
            {
                foreach (var sub in e.InnerExceptions)
                {
                    WriteToConsole("Exception", sub.Message, ConsoleColor.Red);
                }
                WriteToConsole(appName, "Finished with error.");
                Environment.Exit(1);
            }
            catch (Exception e)
            {
                WriteToConsole("Exception", e.Message, ConsoleColor.Red);
                WriteToConsole(appName, "Finished with error.");
                Environment.Exit(1);
            }
        }
    }
}
