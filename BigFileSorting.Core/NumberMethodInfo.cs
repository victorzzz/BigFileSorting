using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;

namespace BigFileSorting.Core
{
    internal static class NumberMethodInfo<TNumber>
    {
        public static readonly MethodInfo TryParseMethodInfo = GetTryParseMethodInfo();
        public static readonly MethodInfo CompareToMethodInfo = GetCompareToMethodInfo();

        #region Private Methods

        private static MethodInfo GetTryParseMethodInfo()
        {
            var type = typeof(TNumber);
            var method = type.GetMethod(
                "TryParse",
                new[]
                {
                    typeof (string),
                    type.MakeByRefType()
                });

            return method;
        }

        private static MethodInfo GetCompareToMethodInfo()
        {
            var type = typeof(TNumber);
            var method = type.GetMethod(
                "CompareTo",
                new[]
                {
                    type
                });

            return method;
        }

        #endregion
    }
}
