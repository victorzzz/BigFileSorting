using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using System.Reflection;
using System.Threading;
using BigFileSorting.Core.Utils;

namespace BigFileSorting.Console.Common
{
    public static class ConsoleAppRunner
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

        public static void ExecuteCommandLineTool(string appName, Action<CancellationToken> action)
        {
            AppDomain.CurrentDomain.UnhandledException += (s, e) =>
            {
                if (e.ExceptionObject is Exception)
                {
                    ConsoleHelper.WriteExceptionToConsole("UnhandledException", (Exception)e.ExceptionObject);
                }
                else
                {
                    ConsoleHelper.WriteToConsole("UnhandledException", e.ExceptionObject.ToString(), ConsoleColor.Red);
                }
                ConsoleHelper.WriteToConsole(appName, "Finished with error.");
            };

            try
            {
                ConsoleHelper.WriteToConsole(appName, "Starting...");

                var cts = new CancellationTokenSource();
                SetConsoleCtrlHandler(new HandlerRoutine(ctrlType =>
                {
                    ConsoleHelper.WriteToConsole(appName, "Console Ctrl handler!!! Application canceled!", ConsoleColor.DarkYellow);
                    cts.Cancel();
                    return true;
                }), true);

                action(cts.Token);

                ConsoleHelper.WriteToConsole(appName, "Finished.");
            }
            catch (AggregateException e)
            {
                ConsoleHelper.WriteToConsole("AggregateException", e.Message, ConsoleColor.Red);
                ConsoleHelper.WriteToConsole("+++++++++++++++++++++++", "+++++++++++++++++++++++", ConsoleColor.DarkRed);
                foreach (var sub in e.InnerExceptions)
                {
                    ConsoleHelper.WriteExceptionToConsole("ExecuteCommandLineTool", sub);
                }
                ConsoleHelper.WriteToConsole(appName, "Finished with error.");
                Environment.Exit(1);
            }
            catch (Exception e)
            {
                ConsoleHelper.WriteExceptionToConsole("ExecuteCommandLineTool", e);

                ConsoleHelper.WriteToConsole(appName, "Finished with error.");
                Environment.Exit(1);
            }
        }
    }
}
