using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

            int minTail = Math.Min(leftTail, rightTail);

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

        public static ulong ParseULongToPosition(int stopPosition)
        {

        }
    }
}
