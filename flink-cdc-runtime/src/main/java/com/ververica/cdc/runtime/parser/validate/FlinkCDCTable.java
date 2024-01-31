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

package com.ververica.cdc.runtime.parser.validate;

import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.runtime.typeutils.DataTypeConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

/** FlinkCDCTable to generate the metadata of calcite. */
public class FlinkCDCTable extends AbstractTable {

    private String name;

    private List<Column> columns;

    public FlinkCDCTable(String name, List<Column> columns) {
        this.name = name;
        this.columns = columns;
    }

    public String getName() {
        return name;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        List<String> names = new ArrayList<>();
        List<RelDataType> types = new ArrayList<>();
        for (Column column : columns) {
            names.add(column.getName());
            RelDataType sqlType =
                    relDataTypeFactory.createSqlType(
                            DataTypeConverter.convertCalciteType(column.getType()));
            types.add(sqlType);
        }
        return relDataTypeFactory.createStructType(Pair.zip(names, types));
    }
}