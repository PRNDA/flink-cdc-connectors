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

package com.ververica.cdc.connectors.doris.sink;

import com.ververica.cdc.common.data.ArrayData;
import com.ververica.cdc.common.data.GenericArrayData;
import com.ververica.cdc.common.data.GenericMapData;
import com.ververica.cdc.common.data.MapData;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.types.DataField;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DecimalType;
import com.ververica.cdc.common.types.LocalZonedTimestampType;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.common.types.TimestampType;
import com.ververica.cdc.common.types.ZonedTimestampType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** converter {@link RecordData} type object to doris field. */
public class DorisRowConverter implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /** Format DATE type data. */
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");
    /** Runtime converter to convert {@link RecordData} type object to doris field. */
    @FunctionalInterface
    interface SerializationConverter extends Serializable {
        Object serialize(int index, RecordData field);
    }

    static SerializationConverter createNullableExternalConverter(DataType type) {
        return wrapIntoNullableExternalConverter(createExternalConverter(type));
    }

    static SerializationConverter wrapIntoNullableExternalConverter(
            SerializationConverter serializationConverter) {
        return (index, val) -> {
            if (val == null || val.isNullAt(index)) {
                return null;
            } else {
                return serializationConverter.serialize(index, val);
            }
        };
    }

    static SerializationConverter createExternalConverter(DataType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return (index, val) -> val.getString(index).toString();
            case BOOLEAN:
                return (index, val) -> val.getBoolean(index);
            case BINARY:
            case VARBINARY:
                return (index, val) -> val.getBinary(index);
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (index, val) ->
                        val.getDecimal(index, decimalPrecision, decimalScale).toBigDecimal();
            case TINYINT:
                return (index, val) -> val.getByte(index);
            case SMALLINT:
                return (index, val) -> val.getShort(index);
            case INTEGER:
                return (index, val) -> val.getInt(index);
            case BIGINT:
                return (index, val) -> val.getLong(index);
            case FLOAT:
                return (index, val) -> val.getFloat(index);
            case DOUBLE:
                return (index, val) -> val.getDouble(index);
            case DATE:
                return (index, val) ->
                        DATE_FORMATTER.format(Date.valueOf(
                                LocalDate.ofEpochDay(val.getInt(index))));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                return (index, val) -> val.getTimestamp(index, timestampPrecision).toTimestamp();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int localP = ((LocalZonedTimestampType) type).getPrecision();
                return (index, val) -> val.getTimestamp(index, localP).toTimestamp();
            case TIMESTAMP_WITH_TIME_ZONE:
                final int zonedP = ((ZonedTimestampType) type).getPrecision();
                return (index, val) -> val.getTimestamp(index, zonedP).toTimestamp();
            case ARRAY:
                return (index, val) -> convertArrayData(val.getArray(index), type);
            case MAP:
                return (index, val) -> writeValueAsString(convertMapData(val.getMap(index), type));
            case ROW:
                return (index, val) -> writeValueAsString(convertRowData(val, index, type));
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    private static List<Object> convertArrayData(ArrayData array, DataType type) {
        if (array instanceof GenericArrayData) {
            return Arrays.asList(((GenericArrayData) array).toObjectArray());
        }
        throw new UnsupportedOperationException("Unsupported array data: " + array.getClass());
    }

    private static Object convertMapData(MapData map, DataType type) {
        Map<Object, Object> result = new HashMap<>();
        if (map instanceof GenericMapData) {
            GenericMapData gMap = (GenericMapData) map;
            for (Object key : ((GenericArrayData) gMap.keyArray()).toObjectArray()) {
                result.put(key, gMap.get(key));
            }
            return result;
        }
        throw new UnsupportedOperationException("Unsupported map data: " + map.getClass());
    }

    private static Object convertRowData(RecordData val, int index, DataType type) {
        RowType rowType = (RowType) type;
        Map<String, Object> value = new HashMap<>();
        RecordData row = val.getRow(index, rowType.getFieldCount());

        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            DataField rowField = fields.get(i);
            SerializationConverter converter = createNullableExternalConverter(rowField.getType());
            Object valTmp = converter.serialize(i, row);
            value.put(rowField.getName(), valTmp.toString());
        }
        return value;
    }

    private static String writeValueAsString(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
