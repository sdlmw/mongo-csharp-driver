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

namespace MongoDB.Bson
{
    /// <summary>
    /// Represents a BSON Decimal128 value.
    /// </summary>
    /// <seealso cref="MongoDB.Bson.BsonValue" />
    public class BsonDecimal128 : BsonValue, IComparable<BsonDecimal128>, IEquatable<BsonDecimal128>
    {
        private readonly Decimal128 _value;

        /// <summary>
        /// Initializes a new instance of the <see cref="BsonDecimal128" /> class.
        /// </summary>
        /// <param name="value">The value.</param>
        public BsonDecimal128(Decimal128 value)
        {
            _value = value;
        }

        /// <inheritdoc />
        public override BsonType BsonType => BsonType.Decimal128;

        /// <summary>
        /// Gets the value.
        /// </summary>
        public Decimal128 Value
        {
            get { return _value; }
        }

        /// <inheritdoc />
        public int CompareTo(BsonDecimal128 other)
        {
            if (other == null) { return 1; }
            return _value.CompareTo(other._value);
        }

        /// <inheritdoc />
        public override int CompareTo(BsonValue other)
        {
            if (other == null) { return 1; }
            var otherDecimal128 = other as BsonDecimal128;
            if (otherDecimal128 != null)
            {
                return CompareTo(otherDecimal128);
            }
            return CompareTypeTo(other);
        }

        /// <inheritdoc />
        public bool Equals(BsonDecimal128 other)
        {
            if (other == null)
            {
                return false;
            }

            return _value.Equals(other._value);
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as BsonDecimal128);
        }

        /// <inheritdoc />
        public override int GetHashCode() => _value.GetHashCode();

        /// <inheritdoc />
        public override string ToString() => _value.ToString();

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.Byte"/> to <see cref="BsonDecimal128"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator BsonDecimal128(byte value)
        {
            return new BsonDecimal128(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.SByte"/> to <see cref="BsonDecimal128"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static implicit operator BsonDecimal128(sbyte value)
        {
            return new BsonDecimal128(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.Int16"/> to <see cref="BsonDecimal128"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator BsonDecimal128(short value)
        {
            return new BsonDecimal128(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.UInt16"/> to <see cref="BsonDecimal128"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static implicit operator BsonDecimal128(ushort value)
        {
            return new BsonDecimal128(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.Int32"/> to <see cref="BsonDecimal128"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator BsonDecimal128(int value)
        {
            return new BsonDecimal128(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.UInt32"/> to <see cref="BsonDecimal128"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static implicit operator BsonDecimal128(uint value)
        {
            return new BsonDecimal128(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.Int64"/> to <see cref="BsonDecimal128"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator BsonDecimal128(long value)
        {
            return new BsonDecimal128(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.UInt64"/> to <see cref="BsonDecimal128"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static implicit operator BsonDecimal128(ulong value)
        {
            return new BsonDecimal128(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.Decimal"/> to <see cref="BsonDecimal128"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator BsonDecimal128(decimal value)
        {
            return new BsonDecimal128(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="Decimal128"/> to <see cref="BsonDecimal128"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator BsonDecimal128(Decimal128 value)
        {
            return new BsonDecimal128(value);
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="BsonDecimal128"/> to <see cref="System.Byte"/>.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator byte(BsonDecimal128 value)
        {
            return (byte)value._value;
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="BsonDecimal128"/> to <see cref="System.SByte"/>.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static explicit operator sbyte(BsonDecimal128 value)
        {
            return (sbyte)value._value;
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="BsonDecimal128"/> to <see cref="System.Int16"/>.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator short(BsonDecimal128 value)
        {
            return (short)value._value;
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="BsonDecimal128"/> to <see cref="System.UInt16"/>.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static explicit operator ushort(BsonDecimal128 value)
        {
            return (ushort)value._value;
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="BsonDecimal128"/> to <see cref="System.Int32"/>.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator int(BsonDecimal128 value)
        {
            return (int)value._value;
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="BsonDecimal128"/> to <see cref="System.UInt32"/>.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static explicit operator uint(BsonDecimal128 value)
        {
            return (uint)value._value;
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="BsonDecimal128"/> to <see cref="System.Int64"/>.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator long(BsonDecimal128 value)
        {
            return (long)value._value;
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="BsonDecimal128"/> to <see cref="System.UInt64"/>.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static explicit operator ulong(BsonDecimal128 value)
        {
            return (ulong)value._value;
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="BsonDecimal128"/> to <see cref="System.Decimal"/>.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator decimal(BsonDecimal128 value)
        {
            return (decimal)value._value;
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="BsonDecimal128"/> to <see cref="Decimal128"/>.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator Decimal128(BsonDecimal128 value)
        {
            return value._value;
        }
    }
}