using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BigFileSorting.Core.Exceptions;

namespace BigFileSorting.Core.Utils
{
    internal static class StringExtension
    {
        public static int LexicographicallyCompareSubstring(this string leftStr, int leftStartPos, string rightStr, int rightStartPos)
        {
            int leftTail = leftStr.Length - leftStartPos;
            int rightTail = rightStr.Length - rightStartPos;

            if (leftTail == 0 && rightTail == 0)
            {
                return 0;
            }

            if (leftTail == 0)
            {
                return -1;
            }

            if (rightTail == 0)
            {
                return 1;
            }

            var minTail = Math.Min(leftTail, rightTail);

            for(int i=0; i < minTail - 1; ++i)       
            {
                var leftChar = leftStr[leftStartPos + i];
                var rightChar = rightStr[rightStartPos + i];


                if (leftChar < rightChar)
                {
                    return -1;
                }

                if (leftChar > rightChar)
                {
                    return 1;
                }
            }

            if (leftTail < rightTail)
            {
                return -1;
            }

            if (leftTail > rightTail)
            {
                return 1;
            }

            return 0;
        }

        public static ulong ParseULongToDelimiter(this string str, char delimiter, out int delimiterPosition)
        {
            if (str[0] == delimiter)
            {
                throw new InvalidFileException("'.' - first character in a line");
            }

            ulong result = 0;
            delimiterPosition = 0;
            for (int i = 0; i < str.Length; ++i)
            {
                var c = str[i];
                if (c == delimiter)
                {
                    delimiterPosition = i;
                    break;
                }

                if (c < '0' || c > '9')
                {
                    throw new InvalidFileException("Non-digit character before '.'");
                }

                result = result * 10ul + (ulong)(c - '0');
            }

            if (delimiterPosition == 0)
            {
                throw new InvalidFileException("No '.' was found for a line");
            }

            return result;
        }
    }
}
