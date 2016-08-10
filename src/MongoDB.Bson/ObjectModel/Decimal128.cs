/* Copyright 2016 MongoDB Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.RegularExpressions;

namespace MongoDB.Bson
{
    /// <summary>
    /// Represents a Decimal128 value.
    /// </summary>
#if NET45
    [Serializable]
#endif
    public struct Decimal128 : IConvertible, IComparable<Decimal128>, IEquatable<Decimal128>
    {
        private const short __maxSignificandDigits = 34;
        private const short __exponentMax = 6111;
        private const short __exponentMin = -6176;
        private const short __exponentBias = 6176;

        private static readonly UInt128 __maxSignificand = UInt128.Parse("9999999999999999999999999999999999");
        private static readonly Decimal128 __maxValue = Decimal128.Parse("9999999999999999999999999999999999E+6111");
        private static readonly Decimal128 __minValue = Decimal128.Parse("-9999999999999999999999999999999999E+6111");

        /// <summary>
        /// Gets the maximum value.
        /// </summary>
        public static Decimal128 MaxValue =>
            __maxValue;

        /// <summary>
        /// Gets the minimum value.
        /// </summary>
        public static Decimal128 MinValue =>
            __minValue;

        /// <summary>
        /// Represents negative infinity.
        /// </summary>
        public static Decimal128 NegativeInfinity =>
            new Decimal128(0xF800000000000000, 0);

        /// <summary>
        /// Represents positive infinity.
        /// </summary>
        public static Decimal128 PositiveInfinity =>
            new Decimal128(0x7800000000000000, 0);

        /// <summary>
        /// Represents a value that is not a number.
        /// </summary>
        public static Decimal128 QNaN =>
            new Decimal128(0x7C00000000000000, 0);

        /// <summary>
        /// Represents a value that is not a number and raises errors when used in calculations.
        /// </summary>
        public static Decimal128 SNaN =>
            new Decimal128(0x7E00000000000000, 0);

        /// <summary>
        /// Represents zero.
        /// </summary>
        public static Decimal128 Zero =>
            new Decimal128(0, 0);

        private readonly byte _flags;
        private readonly short _exponent;
        private readonly UInt128 _significand;

        private Decimal128(byte flags, short exponent, UInt128 significand)
        {
            if (exponent > __exponentMax || exponent < __exponentMin)
            {
                throw new ArgumentException($"Exponent must be between {__exponentMin} and {__exponentMax}.", nameof(exponent));
            }
            _flags = flags;
            _exponent = exponent;
            _significand = significand;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Decimal128"/> struct.
        /// </summary>
        /// <param name="value">The value.</param>
        public Decimal128(decimal value)
        {
            var bits = decimal.GetBits(value);

            _flags = 0;
            if ((bits[3] & 0x80000000) != 0)
            {
                _flags = 0x80;
            }
            _exponent = (short)((bits[3] >> 16) & 0x7F);
            _exponent *= -1;

            var significandHigh = (ulong)(uint)bits[2];
            var significandLow = ((ulong)(uint)bits[1] << 32) | (ulong)(uint)bits[0];
            _significand = new UInt128(significandHigh, significandLow);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Decimal128"/> struct.
        /// </summary>
        /// <param name="value">The value.</param>
        public Decimal128(double value)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Decimal128"/> struct.
        /// </summary>
        /// <param name="value">The value.</param>
        public Decimal128(float value)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Decimal128"/> struct.
        /// </summary>
        /// <param name="value">The value.</param>
        public Decimal128(int value)
        {
            _flags = 0;
            var v = (long)value;
            if (value < 0)
            {
                // sign bit
                _flags = 0x80;
                v = -v;
            }

            _exponent = 0;
            _significand = new UInt128(0, (ulong)v);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Decimal128"/> struct.
        /// </summary>
        /// <param name="value">The value.</param>
        [CLSCompliant(false)]
        public Decimal128(uint value)
        {
            _flags = 0;
            _exponent = 0;
            _significand = new UInt128(0, (ulong)value);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Decimal128"/> struct.
        /// </summary>
        /// <param name="value">The value.</param>
        public Decimal128(long value)
        {
            _flags = 0;
            if (value < 0)
            {
                // sign bit
                _flags = 0x80;
                value = -value;
            }

            _exponent = 0;
            _significand = new UInt128(0, (ulong)value);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Decimal128"/> struct.
        /// </summary>
        /// <param name="value">The value.</param>
        [CLSCompliant(false)]
        public Decimal128(ulong value)
        {
            _flags = 0;
            _exponent = 0;
            _significand = new UInt128(0, (ulong)value);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Decimal128"/> struct.
        /// </summary>
        /// <param name="highBits">The high order 64 bits of the binary representation.</param>
        /// <param name="lowBits">The low order 64 bits of the binary representation.</param>
        [CLSCompliant(false)]
        public Decimal128(ulong highBits, ulong lowBits)
        {
            _flags = (byte)(highBits >> 56);
            ulong significandHigh;
            if ((_flags & 0x78) == 0x78)
            {
                // it's either Infinity or NaN
                _exponent = 0;
                significandHigh = highBits & 0x00ffffffffffffff;
            }
            else if ((_flags & 0x60) == 0x60)
            {
                // the significand starts with an implied 100 and the last bit of the combination field
                _exponent = (short)(((int)(highBits >> 47) & 0x3fff) - __exponentBias);
                significandHigh = 0x0002000000000000 | (highBits & 0x00007fffffffffff);
            }
            else
            {
                // the significand starts with an implied 0 and the last three bits of the combination field
                _exponent = (short)(((int)(highBits >> 49) & 0x3fff) - __exponentBias);
                significandHigh = highBits & 0x0001ffffffffffff;
            }
            _significand = new UInt128(significandHigh, lowBits);
        }

        /// <summary>
        /// Compares two specified Decimal128 values and returns an integer that indicates whether the first value
        /// is greater than, less than, or equal to the second value.
        /// </summary>
        /// <param name="x">The first value.</param>
        /// <param name="y">The second value.</param>
        /// <returns>Less than zero if x &lt; y, zero if x == y, and greater than zero if x &gt; y.</returns>
        public static int Compare(Decimal128 x, Decimal128 y)
        {
            return Decimal128Comparer.Instance.Compare(x, y);
        }

        /// <inheritdoc />
        public int CompareTo(Decimal128 other)
        {
            return Decimal128.Compare(this, other);
        }

        /// <inheritdoc />
        public bool Equals(Decimal128 other)
        {
            return Decimal128.Equals(this, other);
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (obj == null || obj.GetType() != typeof(Decimal128))
            {
                return false;
            }
            else
            {
                return Equals((Decimal128)obj);
            }
        }

        /// <summary>
        /// Determines whether the specified Decimal128 instances are considered equal.
        /// </summary>
        /// <param name="x">The first Decimal128 object to compare.</param>
        /// <param name="y">The second Decimal128 object to compare.</param>
        /// <returns>True if the objects are considered equal; otherwise false. If both x and y are null, the method returns true.</returns>
        public static bool Equals(Decimal128 x, Decimal128 y)
        {
            return Decimal128.Compare(x, y) == 0;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            int hash = 17;
            hash = 37 * hash + _flags.GetHashCode();
            hash = 37 * hash + _exponent.GetHashCode();
            hash = 37 * hash + _significand.GetHashCode();
            return hash;
        }

        /// <summary>
        /// Negates the specified x.
        /// </summary>
        /// <param name="x">The x.</param>
        /// <returns>The result of multiplying the value by negative one.</returns>
        public static Decimal128 Negate(Decimal128 x)
        {
            return new Decimal128((byte)(x._flags ^ 0x80), x._exponent, x._significand);
        }

        /// <inheritdoc />
        public override string ToString()
        {
            if ((_flags & 0x60) == 0x60)
            {
                if (Decimal128.IsPositiveInfinity(this))
                {
                    return "Infinity";
                }
                if (Decimal128.IsNegativeInfinity(this))
                {
                    return "-Infinity";
                }
                if (Decimal128.IsNaN(this))
                {
                    return "NaN";
                }

                // invalid representation treated as zero
                if (_exponent == 0)
                {
                    return (_flags & 0x80) == 0x80 ? "-0" : "0";
                }
                else
                {
                    var exponentString = _exponent.ToString(NumberFormatInfo.InvariantInfo);
                    if (_exponent > 0)
                    {
                        exponentString = "+" + exponentString;
                    }
                    return ((_flags & 0x80) == 0x80 ? "-0E" : "0E") + exponentString;
                }
            }

            var coefficientString = _significand.ToString();
            var adjustedExponent = _exponent + coefficientString.Length - 1;

            string result;
            if (_exponent > 0 || adjustedExponent < -6)
            {
                result = ToStringWithExponentialNotation(coefficientString, adjustedExponent);
            }
            else
            {
                result = ToStringWithoutExponentialNotation(coefficientString, _exponent);
            }

            if ((_flags & 0x80) == 0x80)
            {
                result = "-" + result;
            }

            return result;
        }

        private string ToStringWithExponentialNotation(string coefficientString, int adjustedExponent)
        {
            if (coefficientString.Length > 1)
            {
                coefficientString = coefficientString.Substring(0, 1) + "." + coefficientString.Substring(1);
            }
            var exponentString = adjustedExponent.ToString(NumberFormatInfo.InvariantInfo);
            if (adjustedExponent >= 0)
            {
                exponentString = "+" + exponentString;
            }
            return coefficientString + "E" + exponentString;
        }

        private string ToStringWithoutExponentialNotation(string coefficientString, int exponent)
        {
            if (exponent == 0)
            {
                return coefficientString;
            }
            else
            {
                var exponentAbsoluteValue = Math.Abs(exponent);
                var minimumCoefficientStringLength = exponentAbsoluteValue + 1;
                if (coefficientString.Length < minimumCoefficientStringLength)
                {
                    coefficientString = coefficientString.PadLeft(minimumCoefficientStringLength, '0');
                }
                var decimalPointIndex = coefficientString.Length - exponentAbsoluteValue;
                return coefficientString.Substring(0, decimalPointIndex) + "." + coefficientString.Substring(decimalPointIndex);
            }
        }

        /// <inheritdoc />
        public string ToString(IFormatProvider provider)
        {
            return ToString();
        }

        TypeCode IConvertible.GetTypeCode()
        {
            return TypeCode.Object;
        }

        bool IConvertible.ToBoolean(IFormatProvider provider)
        {
            return !Decimal128.Equals(this, Decimal128.Zero);
        }

        char IConvertible.ToChar(IFormatProvider provider)
        {
            throw new InvalidCastException("Invalid cast from Decima128 to Char.");
        }

        sbyte IConvertible.ToSByte(IFormatProvider provider)
        {
            return Decimal128.ToSByte(this);
        }

        byte IConvertible.ToByte(IFormatProvider provider)
        {
            return Decimal128.ToByte(this);
        }

        short IConvertible.ToInt16(IFormatProvider provider)
        {
            return Decimal128.ToInt16(this);
        }

        ushort IConvertible.ToUInt16(IFormatProvider provider)
        {
            return Decimal128.ToUInt16(this);
        }

        int IConvertible.ToInt32(IFormatProvider provider)
        {
            return Decimal128.ToInt32(this);
        }

        uint IConvertible.ToUInt32(IFormatProvider provider)
        {
            return Decimal128.ToUInt32(this);
        }

        long IConvertible.ToInt64(IFormatProvider provider)
        {
            return Decimal128.ToInt64(this);
        }

        ulong IConvertible.ToUInt64(IFormatProvider provider)
        {
            return Decimal128.ToUInt64(this);
        }

        float IConvertible.ToSingle(IFormatProvider provider)
        {
            return Decimal128.ToSingle(this);
        }

        double IConvertible.ToDouble(IFormatProvider provider)
        {
            return Decimal128.ToDouble(this);
        }

        decimal IConvertible.ToDecimal(IFormatProvider provider)
        {
            return Decimal128.ToDecimal(this);
        }

        DateTime IConvertible.ToDateTime(IFormatProvider provider)
        {
            throw new InvalidCastException("Invalid cast from Decima128 to DateTime.");
        }

        object IConvertible.ToType(Type conversionType, IFormatProvider provider)
        {
            var convertible = (IConvertible)this;
            switch (Type.GetTypeCode(conversionType))
            {
                case TypeCode.Boolean: return convertible.ToBoolean(provider);
                case TypeCode.Byte: return convertible.ToByte(provider);
                case TypeCode.Char: return convertible.ToChar(provider);
                case TypeCode.DateTime: return convertible.ToDateTime(provider);
                case TypeCode.Decimal: return convertible.ToDecimal(provider);
                case TypeCode.Double: return convertible.ToDouble(provider);
                case TypeCode.Int16: return convertible.ToInt16(provider);
                case TypeCode.Int32: return convertible.ToInt32(provider);
                case TypeCode.Int64: return convertible.ToInt64(provider);
                case TypeCode.SByte: return convertible.ToSByte(provider);
                case TypeCode.Single: return convertible.ToSingle(provider);
                case TypeCode.String: return convertible.ToString(provider);
                case TypeCode.UInt16: return convertible.ToUInt16(provider);
                case TypeCode.UInt32: return convertible.ToUInt32(provider);
                case TypeCode.UInt64: return convertible.ToUInt64(provider);
                default: throw new InvalidCastException();
            }
        }

        /// <summary>
        /// Gets the high order 64 bits of the binary representation of this instance.
        /// </summary>
        /// <returns>The high order 64 bits of the binary representation of this instance.</returns>
        [CLSCompliant(false)]
        public ulong GetHighBits()
        {
            var biasedExponent = _exponent + __exponentBias;
            ulong highBits;
            if ((_flags & 0x70) == 0x70)
            {
                // it's either Infinity or NaN
                highBits = _significand.High;
            }
            else if ((_flags & 0x60) == 0x60)
            {
                highBits = ((ulong)biasedExponent << 47) | (_significand.High & 0x00007fffffffffff);
            }
            else
            {
                highBits = ((ulong)biasedExponent << 49) | _significand.High;
            }
            return ((ulong)_flags << 56) | highBits;
        }

        /// <summary>
        /// Gets the low order 64 bits of the binary representation of this instance.
        /// </summary>
        /// <returns>The low order 64 bits of the binary representation of this instance.</returns>
        [CLSCompliant(false)]
        public ulong GetLowBits()
        {
            return _significand.Low;
        }

        /// <summary>
        /// Returns a value indicating whether the specified number evaluates to negative or positive infinity.
        /// </summary>
        /// <param name="d">A 128-bit decimal.</param>
        /// <returns>true if <paramref name="d" /> evaluates to negative or positive infinity; otherwise, false.</returns>
        public static bool IsInfinity(Decimal128 d) => IsPositiveInfinity(d) || IsNegativeInfinity(d);

        /// <summary>
        /// Returns a value indicating whether the specified number is negative.
        /// </summary>
        /// <param name="d">A 128-bit decimal.</param>
        /// <returns>true if <paramref name="d" /> is negative; otherwise, false.</returns>
        public static bool IsNegative(Decimal128 d) => (d._flags & 0x80) != 0;

        /// <summary>
        /// Returns a value indicating whether the specified number evaluates to negative infinity.
        /// </summary>
        /// <param name="d">A 128-bit decimal.</param>
        /// <returns>true if <paramref name="d" /> evaluates to negative infinity; otherwise, false.</returns>
        public static bool IsNegativeInfinity(Decimal128 d) => (d._flags & 0xFC) == 0xF8;

        /// <summary>
        /// Returns a value indicating whether the specified number evaluates to positive infinity.
        /// </summary>
        /// <param name="d">A 128-bit decimal.</param>
        /// <returns>true if <paramref name="d" /> evaluates to positive infinity; otherwise, false.</returns>
        public static bool IsPositiveInfinity(Decimal128 d) => (d._flags & 0xFC) == 0x78;

        /// <summary>
        /// Returns a value indicating whether the specified number is not a number.
        /// </summary>
        /// <param name="d">A 128-bit decimal.</param>
        /// <returns>true if <paramref name="d" /> is not a number; otherwise, false.</returns>
        public static bool IsNaN(Decimal128 d) => IsQNaN(d) || IsSNaN(d);

        /// <summary>
        /// Returns a value indicating whether the specified number is a quiet not a number.
        /// </summary>
        /// <param name="d">A 128-bit decimal.</param>
        /// <returns>true if <paramref name="d" /> is a quiet not a number; otherwise, false.</returns>
        public static bool IsQNaN(Decimal128 d) => (d._flags & 0x7E) == 0x7C;

        /// <summary>
        /// Returns a value indicating whether the specified number is a signaled not a number.
        /// </summary>
        /// <param name="d">A 128-bit decimal.</param>
        /// <returns>true if <paramref name="d" /> is a signaled not a number; otherwise, false.</returns>
        public static bool IsSNaN(Decimal128 d) => (d._flags & 0x7E) == 0x7E;

        /// <summary>
        /// Gets a value indicating whether this instance is zero.
        /// </summary>
        /// <value>
        ///   <c>true</c> if this instance is zero; otherwise, <c>false</c>.
        /// </value>
        public static bool IsZero(Decimal128 d)
        {
            if (d._significand.Equals(UInt128.Zero))
            {
                return true;
            }

            if ((d._flags & 0x60) == 0x60 && (d._flags & 0x78) != 0x78)
            {
                // invalid representations are interpreted as zero
                return true;
            }

            return false;
        }

        /// <summary>
        /// Converts the string representation of a number to its <see cref="Decimal128" /> equivalent.
        /// </summary>
        /// <param name="s">The string representation of the number to convert.</param>
        /// <returns>
        /// The equivalent to the number contained in <paramref name="s" />.
        /// </returns>
        public static Decimal128 Parse(string s)
        {
            Decimal128 value;
            if (!TryParse(s, out value))
            {
                throw new FormatException($"{s} is not a valid Decimal128.");
            }

            return value;
        }

        /// <summary>
        /// Converts the string representation of a number to its <see cref="Decimal128" /> equivalent. A return value indicates whether the conversion succeeded or failed.
        /// </summary>
        /// <param name="s">The string representation of the number to convert.</param>
        /// <param name="result">When this method returns, contains the <see cref="Decimal128" /> number that is equivalent to the numeric value contained in <paramref name="s" />, if the conversion succeeded, or is zero if the conversion failed. The conversion fails if the <paramref name="s" /> parameter is null, is not a number in a valid format, or represents a number less than the min value or greater than the max value. This parameter is passed uninitialized.</param>
        /// <returns>
        /// true if <paramref name="s" /> was converted successfully; otherwise, false.
        /// </returns>
        public static bool TryParse(string s, out Decimal128 result)
        {
            if (s == null || s.Length == 0)
            {
                result = default(Decimal128);
                return false;
            }

            const string pattern =
                @"^(?<sign>[+-])?" +
                @"(?<significand>\d+([.]\d*)?|[.]\d+)" +
                @"(?<exponent>[eE](?<exponentSign>[+-])?(?<exponentDigits>\d+))?$";

            var match = Regex.Match(s, pattern);
            if (!match.Success)
            {
                if (s.Equals("Inf", StringComparison.OrdinalIgnoreCase) || s.Equals("Infinity", StringComparison.OrdinalIgnoreCase) ||
                    s.Equals("+Inf", StringComparison.OrdinalIgnoreCase) || s.Equals("+Infinity", StringComparison.OrdinalIgnoreCase))
                {
                    result = Decimal128.PositiveInfinity;
                    return true;
                }

                if (s.Equals("-Inf", StringComparison.OrdinalIgnoreCase) || s.Equals("-Infinity", StringComparison.OrdinalIgnoreCase))
                {
                    result = Decimal128.NegativeInfinity;
                    return true;
                }

                if (s.Equals("NaN", StringComparison.OrdinalIgnoreCase) || s.Equals("-NaN", StringComparison.OrdinalIgnoreCase))
                {
                    result = Decimal128.QNaN;
                    return true;
                }

                result = default(Decimal128);
                return false;
            }

            var sign = match.Groups["sign"].Value == "-" ? -1 : 0;

            var exponent = 0;
            if (match.Groups["exponent"].Length != 0)
            {
                if (!int.TryParse(match.Groups["exponentDigits"].Value, out exponent))
                {
                    result = default(Decimal128);
                    return false;
                }
                if (match.Groups["exponentSign"].Value == "-")
                {
                    exponent = -exponent;
                }
            }

            var significandString = match.Groups["significand"].Value;

            int decimalPointIndex;
            if ((decimalPointIndex = significandString.IndexOf('.')) != -1)
            {
                exponent -= significandString.Length - (decimalPointIndex + 1);
                significandString = significandString.Substring(0, decimalPointIndex) + significandString.Substring(decimalPointIndex + 1);
            }

            significandString = RemoveLeadingZeroes(significandString);
            significandString = ClampOrRound(ref exponent, significandString);

            if (exponent > __exponentMax || exponent < __exponentMin)
            {
                result = default(Decimal128);
                return false;
            }
            if (significandString.Length > 34)
            {
                result = default(Decimal128);
                return false;
            }

            var flags = sign == -1 ? (byte)0x80 : (byte)0x0;
            var significand = UInt128.Parse(significandString);

            result = new Decimal128(flags, (short)exponent, significand);
            return true;
        }

        /// <summary>
        /// Converts the value of the specified <see cref="Decimal128"/> to the equivalent <see cref="decimal"/>.
        /// </summary>
        /// <param name="d">The number to convert.</param>
        /// <returns>A <see cref="decimal"/> equivalent to <paramref name="d" />.</returns>
        public static decimal ToDecimal(Decimal128 d)
        {
            if ((d._significand.High >> 32) == 0 && d._exponent >= -128 && d._exponent <= 127)
            {
                if (d._flags == 0x80 || d._flags == 0)
                {
                    var scale = (byte)-d._exponent;
                    bool isNegative = d._flags == 0x80; // only the negative bit is set
                    var highMid = (int)d._significand.High;
                    var lowMid = (int)(d._significand.Low >> 32);
                    var low = (int)d._significand.Low;
                    return new decimal(low, lowMid, highMid, isNegative, scale);
                }
            }

            throw new OverflowException("Value was either too large or too small for a Decimal.");
        }

        /// <summary>
        /// Converts the value of the specified <see cref="Decimal128"/> to the equivalent <see cref="double"/>.
        /// </summary>
        /// <param name="d">The number to convert.</param>
        /// <returns>A <see cref="double"/> equivalent to <paramref name="d" />.</returns>
        public static double ToDouble(Decimal128 d)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Converts the value of the specified <see cref="Decimal128"/> to the equivalent <see cref="float"/>.
        /// </summary>
        /// <param name="d">The number to convert.</param>
        /// <returns>A <see cref="float"/> equivalent to <paramref name="d" />.</returns>
        public static float ToSingle(Decimal128 d)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Converts the value of the specified <see cref="Decimal128"/> to the equivalent 8-bit unsigned integer.
        /// </summary>
        /// <param name="d">The number to convert.</param>
        /// <returns>A 8-bit unsigned integer equivalent to <paramref name="d" />.</returns>
        public static byte ToByte(Decimal128 d)
        {
            uint num;
            try
            {
                num = ToUInt32(d);
            }
            catch (OverflowException ex)
            {
                throw new OverflowException("Value was either too large or too small for a Byte.", ex);
            }

            if (num < byte.MinValue || num > byte.MaxValue)
            {
                throw new OverflowException("Value was either too large or too small for a Byte.");
            }

            return (byte)num;
        }

        /// <summary>
        /// Converts the value of the specified <see cref="Decimal128"/> to the equivalent 16-bit signed integer.
        /// </summary>
        /// <param name="d">The number to convert.</param>
        /// <returns>A 16-bit signed integer equivalent to <paramref name="d" />.</returns>
        public static short ToInt16(Decimal128 d)
        {
            int num;
            try
            {
                num = ToInt32(d);
            }
            catch (OverflowException ex)
            {
                throw new OverflowException("Value was either too large or too small for a Int16.", ex);
            }

            if (num < short.MinValue || num > short.MaxValue)
            {
                throw new OverflowException("Value was either too large or too small for a Int16.");
            }

            return (short)num;
        }

        /// <summary>
        /// Converts the value of the specified <see cref="Decimal128"/> to the equivalent 32-bit signed integer.
        /// </summary>
        /// <param name="d">The number to convert.</param>
        /// <returns>A 32-bit signed integer equivalent to <paramref name="d" />.</returns>
        public static int ToInt32(Decimal128 d)
        {
            if (d._significand.High == 0 && (d._significand.Low >> 32) == 0)
            {
                int num = (int)d._significand.Low;
                if (d._flags == 0x80) // only the negative bit is set...
                {
                    num = -num;
                    if (num <= 0)
                    {
                        return num;
                    }
                }
                else if (d._flags == 0)
                {
                    if (num >= 0)
                    {
                        return num;
                    }
                }
            }

            throw new OverflowException("Value was either too large or too small for a Int32.");
        }

        /// <summary>
        /// Converts the value of the specified <see cref="Decimal128"/> to the equivalent 64-bit signed integer.
        /// </summary>
        /// <param name="d">The number to convert.</param>
        /// <returns>A 64-bit signed integer equivalent to <paramref name="d" />.</returns>
        public static long ToInt64(Decimal128 d)
        {
            if (d._significand.High == 0)
            {
                long num = (long)d._significand.Low;
                if (d._flags == 0x80) // only the negative bit is set...
                {
                    num = -num;
                    if (num <= 0L)
                    {
                        return num;
                    }
                }
                else if (d._flags == 0)
                {
                    if (num >= 0L)
                    {
                        return num;
                    }
                }
            }

            throw new OverflowException("Value was either too large or too small for a Int64.");
        }

        /// <summary>
        /// Converts the value of the specified <see cref="Decimal128"/> to the equivalent 8-bit signed integer.
        /// </summary>
        /// <param name="d">The number to convert.</param>
        /// <returns>A 8-bit signed integer equivalent to <paramref name="d" />.</returns>
        [CLSCompliant(false)]
        public static sbyte ToSByte(Decimal128 d)
        {
            int num;
            try
            {
                num = ToInt32(d);
            }
            catch (OverflowException ex)
            {
                throw new OverflowException("Value was either too large or too small for a UInt16.", ex);
            }

            if (num < sbyte.MinValue || num > sbyte.MaxValue)
            {
                throw new OverflowException("Value was either too large or too small for a UInt16.");
            }

            return (sbyte)num;
        }

        /// <summary>
        /// Converts the value of the specified <see cref="Decimal128"/> to the equivalent 16-bit unsigned integer.
        /// </summary>
        /// <param name="d">The number to convert.</param>
        /// <returns>A 16-bit unsigned integer equivalent to <paramref name="d" />.</returns>
        [CLSCompliant(false)]
        public static ushort ToUInt16(Decimal128 d)
        {
            uint num;
            try
            {
                num = ToUInt32(d);
            }
            catch (OverflowException ex)
            {
                throw new OverflowException("Value was either too large or too small for a UInt16.", ex);
            }

            if (num < 0u || num > ushort.MaxValue)
            {
                throw new OverflowException("Value was either too large or too small for a UInt16.");
            }

            return (ushort)num;
        }

        /// <summary>
        /// Converts the value of the specified <see cref="Decimal128"/> to the equivalent 32-bit unsigned integer.
        /// </summary>
        /// <param name="d">The number to convert.</param>
        /// <returns>A 32-bit unsigned integer equivalent to <paramref name="d" />.</returns>
        [CLSCompliant(false)]
        public static uint ToUInt32(Decimal128 d)
        {
            if (d._significand.High == 0 && (d._significand.Low >> 32) == 0)
            {
                uint num = (uint)d._significand.Low;
                if (d._flags == 0 || num == 0)
                {
                    return num;
                }
            }

            throw new OverflowException("Value was either too large or too small for a UInt32.");
        }

        /// <summary>
        /// Converts the value of the specified <see cref="Decimal128"/> to the equivalent 64-bit unsigned integer.
        /// </summary>
        /// <param name="d">The number to convert.</param>
        /// <returns>A 64-bit unsigned integer equivalent to <paramref name="d" />.</returns>
        [CLSCompliant(false)]
        public static ulong ToUInt64(Decimal128 d)
        {
            if (d._significand.High == 0)
            {
                ulong num = (ulong)d._significand.Low;
                if (d._flags == 0 || num == 0)
                {
                    return num;
                }
            }

            throw new OverflowException("Value was either too large or too small for a UInt64.");
        }

        /// <summary>
        /// Implements the operator ==.
        /// </summary>
        /// <param name="lhs">The LHS.</param>
        /// <param name="rhs">The RHS.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator ==(Decimal128 lhs, Decimal128 rhs)
        {
            if (Decimal128.IsNaN(lhs) || Decimal128.IsNaN(rhs))
            {
                return false;
            }
            else
            {
                return lhs.Equals(rhs);
            }
        }

        /// <summary>
        /// Implements the operator !=.
        /// </summary>
        /// <param name="lhs">The LHS.</param>
        /// <param name="rhs">The RHS.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator !=(Decimal128 lhs, Decimal128 rhs)
        {
            return !(lhs == rhs);
        }

        /// <summary>
        /// Returns a value indicating whether a specified Decimal128 is greater than another specified Decimal128.
        /// </summary>
        /// <param name="x">The first value.</param>
        /// <param name="y">The second value.</param>
        /// <returns>
        /// true if x &gt; y; otherwise, false.
        /// </returns>
        public static bool operator >(Decimal128 x, Decimal128 y)
        {
            return Decimal128.Compare(x, y) > 0;
        }

        /// <summary>
        /// Returns a value indicating whether a specified Decimal128 is greater than or equal to another another specified Decimal128.
        /// </summary>
        /// <param name="x">The first value.</param>
        /// <param name="y">The second value.</param>
        /// <returns>
        /// true if x &gt;= y; otherwise, false.
        /// </returns>
        public static bool operator >=(Decimal128 x, Decimal128 y)
        {
            return Decimal128.Compare(x, y) >= 0;
        }

        /// <summary>
        /// Returns a value indicating whether a specified Decimal128 is less than another specified Decimal128.
        /// </summary>
        /// <param name="x">The first value.</param>
        /// <param name="y">The second value.</param>
        /// <returns>
        /// true if x &lt; y; otherwise, false.
        /// </returns>
        public static bool operator <(Decimal128 x, Decimal128 y)
        {
            return Decimal128.Compare(x, y) < 0;
        }

        /// <summary>
        /// Returns a value indicating whether a specified Decimal128 is less than or equal to another another specified Decimal128.
        /// </summary>
        /// <param name="x">The first value.</param>
        /// <param name="y">The second value.</param>
        /// <returns>
        /// true if x &lt;= y; otherwise, false.
        /// </returns>
        public static bool operator <=(Decimal128 x, Decimal128 y)
        {
            return Decimal128.Compare(x, y) <= 0;
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.Byte"/> to <see cref="Decimal128"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator Decimal128(byte value)
        {
            return new Decimal128(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.SByte"/> to <see cref="Decimal128"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static implicit operator Decimal128(sbyte value)
        {
            return new Decimal128(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.Int16"/> to <see cref="Decimal128"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator Decimal128(short value)
        {
            return new Decimal128(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.UInt16"/> to <see cref="Decimal128"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static implicit operator Decimal128(ushort value)
        {
            return new Decimal128(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.Int32"/> to <see cref="Decimal128"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator Decimal128(int value)
        {
            return new Decimal128(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.UInt32"/> to <see cref="Decimal128"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static implicit operator Decimal128(uint value)
        {
            return new Decimal128(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.Int64"/> to <see cref="Decimal128"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator Decimal128(long value)
        {
            return new Decimal128(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.UInt64"/> to <see cref="Decimal128"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static implicit operator Decimal128(ulong value)
        {
            return new Decimal128(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.Decimal"/> to <see cref="Decimal128"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator Decimal128(decimal value)
        {
            return new Decimal128(value);
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="double"/> to <see cref="Decimal128"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator Decimal128(double value)
        {
            return new Decimal128(value);
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="float"/> to <see cref="Decimal128"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator Decimal128(float value)
        {
            return new Decimal128(value);
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Decimal128"/> to <see cref="System.Byte"/>.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator byte(Decimal128 value)
        {
            return ToByte(value);
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Decimal128"/> to <see cref="char"/>.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator char(Decimal128 value)
        {
            return (char)ToUInt16(value);
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Decimal128"/> to <see cref="System.SByte"/>.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static explicit operator sbyte(Decimal128 value)
        {
            return ToSByte(value);
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Decimal128"/> to <see cref="System.Int16"/>.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator short(Decimal128 value)
        {
            return ToInt16(value);
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Decimal128"/> to <see cref="System.UInt16"/>.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static explicit operator ushort(Decimal128 value)
        {
            return ToUInt16(value);
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Decimal128"/> to <see cref="System.Int32"/>.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator int(Decimal128 value)
        {
            return ToInt32(value);
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Decimal128"/> to <see cref="System.UInt32"/>.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static explicit operator uint(Decimal128 value)
        {
            return ToUInt32(value);
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Decimal128"/> to <see cref="System.Int64"/>.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator long(Decimal128 value)
        {
            return ToInt64(value);
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Decimal128"/> to <see cref="System.UInt64"/>.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static explicit operator ulong(Decimal128 value)
        {
            return ToUInt64(value);
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Decimal128"/> to <see cref="System.Decimal"/>.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator decimal(Decimal128 value)
        {
            return ToDecimal(value);
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Decimal128"/> to <see cref="double"/>.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator double(Decimal128 value)
        {
            return Decimal128.ToDouble(value);
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Decimal128"/> to <see cref="float"/>.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator float(Decimal128 value)
        {
            return Decimal128.ToSingle(value);
        }

        // private methods
        private static string ClampOrRound(ref int exponent, string significandString)
        {
            if (exponent > __exponentMax)
            {
                if (significandString == "0")
                {
                    // since significand is zero simply use the largest possible exponent
                    exponent = __exponentMax;
                }
                else
                {
                    // use clamping to bring the exponent into range
                    var numberOfTrailingZeroesToAdd = exponent - __exponentMax;
                    var digitsAvailable = 34 - significandString.Length;
                    if (numberOfTrailingZeroesToAdd <= digitsAvailable)
                    {
                        exponent = __exponentMax;
                        significandString = significandString + new string('0', numberOfTrailingZeroesToAdd);
                    }
                }
            }
            else if (exponent < __exponentMin)
            {
                if (significandString == "0")
                {
                    // since significand is zero simply use the smallest possible exponent
                    exponent = __exponentMin;
                }
                else
                {
                    // use exact rounding to bring the exponent into range
                    var numberOfTrailingZeroesToRemove = __exponentMin - exponent;
                    if (numberOfTrailingZeroesToRemove < significandString.Length)
                    {
                        var trailingDigits = significandString.Substring(significandString.Length - numberOfTrailingZeroesToRemove);
                        if (Regex.IsMatch(trailingDigits, "^0+$"))
                        {
                            exponent = __exponentMin;
                            significandString = significandString.Substring(0, significandString.Length - numberOfTrailingZeroesToRemove);
                        }
                    }
                }
            }
            else if (significandString.Length > 34)
            {
                // use exact rounding to reduce significand to 34 digits
                var numberOfTrailingZeroesToRemove = significandString.Length - 34;
                if (exponent + numberOfTrailingZeroesToRemove <= __exponentMax)
                {
                    var trailingDigits = significandString.Substring(significandString.Length - numberOfTrailingZeroesToRemove);
                    if (Regex.IsMatch(trailingDigits, "^0+$"))
                    {
                        exponent += numberOfTrailingZeroesToRemove;
                        significandString = significandString.Substring(0, significandString.Length - numberOfTrailingZeroesToRemove);
                    }
                }
            }

            return significandString;
        }

        private static string RemoveLeadingZeroes(string significandString)
        {
            if (significandString[0] == '0' && significandString.Length > 1)
            {
                significandString = Regex.Replace(significandString, "^0+", "");
                return significandString.Length == 0 ? "0" : significandString;
            }
            else
            {
                return significandString;
            }
        }

        // nested types
        private class Decimal128Comparer : IComparer<Decimal128>
        {
            #region static
            // private static fields
            private static readonly Decimal128Comparer __instance = new Decimal128Comparer();

            // public static properties
            public static Decimal128Comparer Instance
            {
                get { return __instance; }
            }
            #endregion

            // public methods
            public int Compare(Decimal128 x, Decimal128 y)
            {
                var xType = GetDecimal128Type(x);
                var yType = GetDecimal128Type(y);
                var result = xType.CompareTo(yType);
                if (result == 0 && xType == Decimal128Type.Number)
                {
                    return CompareNumbers(x, y);
                }
                else
                {
                    return result;
                }
            }

            // private methods
            private Decimal128Type GetDecimal128Type(Decimal128 x)
            {
                if (Decimal128.IsNaN(x)) { return Decimal128Type.NaN; }
                else if (Decimal128.IsNegativeInfinity(x)) { return Decimal128Type.NegativeInfinity; }
                else if (Decimal128.IsPositiveInfinity(x)) { return Decimal128Type.PositiveInfity; }
                else { return Decimal128Type.Number; }
            }

            private int CompareNumbers(Decimal128 x, Decimal128 y)
            {
                var xClass = GetNumberClass(x);
                var yClass = GetNumberClass(y);
                var result = xClass.CompareTo(yClass);
                if (result == 0)
                {
                    if (xClass == NumberClass.Negative)
                    {
                        return CompareNegativeNumbers(x, y);
                    }
                    else if (xClass == NumberClass.Positive)
                    {
                        return ComparePositiveNumbers(x, y);
                    }
                    else
                    {
                        return 0; // else all Zeroes compare equal
                    }
                }
                else
                {
                    return result;
                }
            }

            private NumberClass GetNumberClass(Decimal128 x)
            {
                if (Decimal128.IsZero(x)) { return NumberClass.Zero; } // must test for Zero first
                else if (Decimal128.IsNegative(x)) { return NumberClass.Negative; }
                else { return NumberClass.Positive; }
            }

            private int CompareNegativeNumbers(Decimal128 x, Decimal128 y)
            {
                return -ComparePositiveNumbers(Decimal128.Negate(x), Decimal128.Negate(y));
            }

            private int ComparePositiveNumbers(Decimal128 x, Decimal128 y)
            {
                var exponentDifference = Math.Abs(x._exponent - y._exponent);
                if (exponentDifference <= 66)
                {
                    // we may or may not be able to make the exponents equal but we won't know until we try
                    // but we do know we can't eliminate an exponent difference larger than 66
                    if (x._exponent < y._exponent)
                    {
                        x = IncreaseExponent(x, y._exponent);
                        y = DecreaseExponent(y, x._exponent);
                    }
                    else if (x._exponent > y._exponent)
                    {
                        x = DecreaseExponent(x, y._exponent);
                        y = IncreaseExponent(y, x._exponent);
                    }
                }

                if (x._exponent == y._exponent)
                {
                    return x._significand.CompareTo(y._significand);
                }
                else
                {
                    return x._exponent.CompareTo(y._exponent);
                }
            }

            private Decimal128 DecreaseExponent(Decimal128 x, short goal)
            {
                if (Decimal128.IsZero(x))
                {
                    // return a zero with the desired exponent
                    return CreatePositiveDecimal128(goal, UInt128.Zero);
                }

                var exponent = x._exponent;
                var significand = x._significand;
                while (exponent > goal)
                {
                    var significandTimes10 = UInt128.Multiply(significand, (uint)10);
                    if (significandTimes10.CompareTo(Decimal128.__maxSignificand) <= 0)
                    {
                        break;
                    }
                    exponent -= 1;
                    significand = significandTimes10;
                }

                return CreatePositiveDecimal128(exponent, significand);
            }

            private Decimal128 IncreaseExponent(Decimal128 x, short goal)
            {
                if (Decimal128.IsZero(x))
                {
                    // return a zero with the desired exponent
                    return CreatePositiveDecimal128(goal, UInt128.Zero);
                }

                var exponent = x._exponent;
                var significand = x._significand;
                while (exponent < goal)
                {
                    uint remainder;
                    var significandDividedBy10 = UInt128.Divide(significand, (uint)10, out remainder);
                    if (remainder != 0)
                    {
                        break;
                    }
                    exponent += 1;
                    significand = significandDividedBy10;
                }

                return CreatePositiveDecimal128(exponent, significand);
            }

            private Decimal128 CreatePositiveDecimal128(short exponent, UInt128 significand)
            {
                var exponentTwoHighOrderBits = (exponent >> 12) & 0x03;
                var flags = (byte)(exponentTwoHighOrderBits << 5);
                return new Decimal128(flags, exponent, significand);
            }

            private enum Decimal128Type { NaN, NegativeInfinity, Number, PositiveInfity }; // the order matters
            private enum NumberClass { Negative, Zero, Positive }; // the order matters
        }
    }
}
