﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

using BigFileSorting.Core.Utils;

namespace BigFileSorting.Core
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal struct FileRecord : IComparable<FileRecord>, IEquatable<FileRecord>
    {
        public ulong Number { get; }
        public string LineString { get; }

        public int StrStartPosition { get; }

        public FileRecord(ulong number, string lineString, int strStartPosition)
        {
            Number = number;
            LineString = lineString;
            StrStartPosition = strStartPosition;
        }

        int IComparable<FileRecord>.CompareTo(FileRecord other)
        {
            if (Number < other.Number)
            {
                return -1;
            }

            if (Number > other.Number)
            {
                return 1;
            }

            LineString.
        }

        bool IEquatable<FileRecord>.Equals(FileRecord other)
        {
            return Number == other.Number && Str.Equals(other.Str, StringComparison.Ordinal);
        }
    }
}
