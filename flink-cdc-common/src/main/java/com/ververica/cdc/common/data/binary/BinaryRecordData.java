/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.common.data.binary;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.data.ArrayData;
import com.ververica.cdc.common.data.DecimalData;
import com.ververica.cdc.common.data.LocalZonedTimestampData;
import com.ververica.cdc.common.data.MapData;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.data.StringData;
import com.ververica.cdc.common.data.TimestampData;
import com.ververica.cdc.common.data.ZonedTimestampData;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DecimalType;
import com.ververica.cdc.common.types.LocalZonedTimestampType;
import com.ververica.cdc.common.types.TimestampType;
import com.ververica.cdc.common.types.ZonedTimestampType;

import java.nio.ByteOrder;

import static com.ververica.cdc.common.types.DataTypeRoot.DECIMAL;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * An implementation of {@link RecordData} which is backed by {@link MemorySegment} instead of
 * Object. It can significantly reduce the serialization/deserialization of Java objects.
 *
 * <p>A BinaryRecordData has two part: Fixed-length part and variable-length part.
 *
 * <p>Fixed-length part contains 1 byte header and null bit set and field values. Null bit set is
 * used for null tracking and is aligned to 8-byte word boundaries. `Field values` holds
 * fixed-length primitive types and variable-length values which can be stored in 8 bytes inside. If
 * it do not fit the variable-length field, then store the length and offset of variable-length
 * part.
 *
 * <p>Fixed-length part will certainly fall into a MemorySegment, which will speed up the read and
 * write of field. During the write phase, if the target memory segment has less space than fixed
 * length part size, we will skip the space. So the number of fields in a single Row cannot exceed
 * the capacity of a single MemorySegment, if there are too many fields, we suggest that user set a
 * bigger pageSize of MemorySegment.
 *
 * <p>Variable-length part may fall into multiple MemorySegments.
 */
@Internal
public final class BinaryRecordData extends BinarySection implements RecordData, NullAwareGetters {

    public static final boolean LITTLE_ENDIAN =
            (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);
    private static final long FIRST_BYTE_ZERO = LITTLE_ENDIAN ? ~0xFFL : ~(0xFFL << 56L);
    public static final int HEADER_SIZE_IN_BITS = 8;

    public static final String TIMESTAMP_DELIMITER = "//";

    public static int calculateBitSetWidthInBytes(int arity) {
        return ((arity + 63 + HEADER_SIZE_IN_BITS) / 64) * 8;
    }

    public static int calculateFixPartSizeInBytes(int arity) {
        return calculateBitSetWidthInBytes(arity) + 8 * arity;
    }

    /**
     * If it is a fixed-length field, we can call this BinaryRecordData's setXX method for in-place
     * updates. If it is variable-length field, can't use this method, because the underlying data
     * is stored continuously.
     */
    public static boolean isInFixedLengthPart(DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return true;
            case DECIMAL:
                return DecimalData.isCompact(((DecimalType) type).getPrecision());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampData.isCompact(((TimestampType) type).getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return LocalZonedTimestampData.isCompact(
                        ((LocalZonedTimestampType) type).getPrecision());
            case TIMESTAMP_WITH_TIME_ZONE:
                return ZonedTimestampData.isCompact(((ZonedTimestampType) type).getPrecision());
            default:
                return false;
        }
    }

    public static boolean isMutable(DataType type) {
        return isInFixedLengthPart(type) || type.getTypeRoot() == DECIMAL;
    }

    private final int arity;
    private final int nullBitsSizeInBytes;

    public BinaryRecordData(int arity) {
        checkArgument(arity >= 0);
        this.arity = arity;
        this.nullBitsSizeInBytes = calculateBitSetWidthInBytes(arity);
    }

    private int getFieldOffset(int pos) {
        return offset + nullBitsSizeInBytes + pos * 8;
    }

    private void assertIndexIsValid(int index) {
        assert index >= 0 : "index (" + index + ") should >= 0";
        assert index < arity : "index (" + index + ") should < " + arity;
    }

    public int getFixedLengthPartSize() {
        return nullBitsSizeInBytes + 8 * arity;
    }

    @Override
    public int getArity() {
        return arity;
    }

    @Override
    public boolean isNullAt(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.bitGet(segments[0], offset, pos + HEADER_SIZE_IN_BITS);
    }

    @Override
    public boolean getBoolean(int pos) {
        assertIndexIsValid(pos);
        return segments[0].getBoolean(getFieldOffset(pos));
    }

    @Override
    public byte getByte(int pos) {
        assertIndexIsValid(pos);
        return segments[0].get(getFieldOffset(pos));
    }

    @Override
    public short getShort(int pos) {
        assertIndexIsValid(pos);
        return segments[0].getShort(getFieldOffset(pos));
    }

    @Override
    public int getInt(int pos) {
        assertIndexIsValid(pos);
        return segments[0].getInt(getFieldOffset(pos));
    }

    @Override
    public long getLong(int pos) {
        assertIndexIsValid(pos);
        return segments[0].getLong(getFieldOffset(pos));
    }

    @Override
    public float getFloat(int pos) {
        assertIndexIsValid(pos);
        return segments[0].getFloat(getFieldOffset(pos));
    }

    @Override
    public double getDouble(int pos) {
        assertIndexIsValid(pos);
        return segments[0].getDouble(getFieldOffset(pos));
    }

    @Override
    public StringData getString(int pos) {
        assertIndexIsValid(pos);
        int fieldOffset = getFieldOffset(pos);
        final long offsetAndLen = segments[0].getLong(fieldOffset);
        return BinarySegmentUtils.readStringData(segments, offset, fieldOffset, offsetAndLen);
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        assertIndexIsValid(pos);

        if (DecimalData.isCompact(precision)) {
            return DecimalData.fromUnscaledLong(
                    segments[0].getLong(getFieldOffset(pos)), precision, scale);
        }

        int fieldOffset = getFieldOffset(pos);
        final long offsetAndSize = segments[0].getLong(fieldOffset);
        return BinarySegmentUtils.readDecimalData(
                segments, offset, offsetAndSize, precision, scale);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        assertIndexIsValid(pos);

        if (TimestampData.isCompact(precision)) {
            return TimestampData.fromMillis(segments[0].getLong(getFieldOffset(pos)));
        }

        int fieldOffset = getFieldOffset(pos);
        final long offsetAndNanoOfMilli = segments[0].getLong(fieldOffset);
        return BinarySegmentUtils.readTimestampData(segments, offset, offsetAndNanoOfMilli);
    }

    @Override
    public ZonedTimestampData getZonedTimestamp(int pos, int precision) {
        String[] parts = getString(pos).toString().split(TIMESTAMP_DELIMITER);
        return ZonedTimestampData.of(
                Long.parseLong(parts[0]), Integer.parseInt(parts[1]), parts[2]);
    }

    @Override
    public LocalZonedTimestampData getLocalZonedTimestampData(int pos, int precision) {
        assertIndexIsValid(pos);

        if (LocalZonedTimestampData.isCompact(precision)) {
            return LocalZonedTimestampData.fromEpochMillis(
                    segments[0].getLong(getFieldOffset(pos)));
        }

        int fieldOffset = getFieldOffset(pos);
        final long offsetAndNanoOfMilli = segments[0].getLong(fieldOffset);
        return BinarySegmentUtils.readLocalZonedTimestampData(
                segments, offset, offsetAndNanoOfMilli);
    }

    @Override
    public byte[] getBinary(int pos) {
        assertIndexIsValid(pos);
        int fieldOffset = getFieldOffset(pos);
        final long offsetAndLen = segments[0].getLong(fieldOffset);
        return BinarySegmentUtils.readBinary(segments, offset, fieldOffset, offsetAndLen);
    }

    @Override
    public ArrayData getArray(int pos) {
        throw new UnsupportedOperationException("Not support ArrayData");
    }

    @Override
    public MapData getMap(int pos) {
        throw new UnsupportedOperationException("Not support MapData.");
    }

    @Override
    public RecordData getRow(int pos, int numFields) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.readRecordData(segments, numFields, offset, getLong(pos));
    }

    /** The bit is 1 when the field is null. Default is 0. */
    @Override
    public boolean anyNull() {
        // Skip the header.
        if ((segments[0].getLong(0) & FIRST_BYTE_ZERO) != 0) {
            return true;
        }
        for (int i = 8; i < nullBitsSizeInBytes; i += 8) {
            if (segments[0].getLong(i) != 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean anyNull(int[] fields) {
        for (int field : fields) {
            if (isNullAt(field)) {
                return true;
            }
        }
        return false;
    }

    public BinaryRecordData copy() {
        return copy(new BinaryRecordData(arity));
    }

    public BinaryRecordData copy(BinaryRecordData reuse) {
        return copyInternal(reuse);
    }

    private BinaryRecordData copyInternal(BinaryRecordData reuse) {
        byte[] bytes = BinarySegmentUtils.copyToBytes(segments, offset, sizeInBytes);
        reuse.pointTo(MemorySegmentFactory.wrap(bytes), 0, sizeInBytes);
        return reuse;
    }

    public void clear() {
        segments = null;
        offset = 0;
        sizeInBytes = 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        // both BinaryRecordData and NestedRowData have the same memory format
        if (!(o instanceof BinaryRecordData)) {
            return false;
        }
        final BinarySection that = (BinarySection) o;
        return sizeInBytes == that.sizeInBytes
                && BinarySegmentUtils.equals(
                        segments, offset, that.segments, that.offset, sizeInBytes);
    }

    @Override
    public int hashCode() {
        return BinarySegmentUtils.hashByWords(segments, offset, sizeInBytes);
    }

    public void setTotalSize(int sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
    }
}
