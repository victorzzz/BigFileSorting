using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigFileSorting.Core.Utils
{
    public static class ConsoleHelper
    {
        public static void WriteToConsole(string operation, string message, ConsoleColor color = ConsoleColor.White)
        {
            System.Console.ForegroundColor = color;
            System.Console.WriteLine("{0:dd.MM.yyyy HH:mm:ss} | {1} | {2}", DateTime.Now,
                operation, message);
        }

        public static void WriteExceptionToConsole(string operation, Exception e)
        {
            WriteToConsole($"Exception in '{operation}'", e.Message, ConsoleColor.Red);
            WriteToConsole("Stack trace: ", e.StackTrace, ConsoleColor.Yellow);
            WriteToConsole("==============================", "==============================", ConsoleColor.DarkRed);

            if (e.InnerException != null)
            {
                WriteToConsole($"INNER exception in '{operation}'", e.InnerException.Message, ConsoleColor.Red);
                WriteToConsole("Stack trace: ", e.InnerException.StackTrace, ConsoleColor.Yellow);
                WriteToConsole("==============================", "==============================", ConsoleColor.DarkRed);
            }
        }
    }
}
