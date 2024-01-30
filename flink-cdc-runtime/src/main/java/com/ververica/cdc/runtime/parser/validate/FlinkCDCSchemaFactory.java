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
import com.ververica.cdc.common.utils.StringUtils;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** FlinkCDCSchemaFactory to generate the metadata of calcite. */
public class FlinkCDCSchemaFactory implements SchemaFactory {

    public static final FlinkCDCSchemaFactory INSTANCE = new FlinkCDCSchemaFactory();

    private FlinkCDCSchemaFactory() {}

    @Override
    public Schema create(SchemaPlus schemaPlus, String schemaName, Map<String, Object> operand) {
        if (StringUtils.isNullOrWhitespaceOnly(schemaName)) {
            schemaName = "default_schema";
        }
        String tableName = String.valueOf(operand.get("tableName"));
        List<Column> columns = (List<Column>) operand.get("columns");
        return new FlinkCDCSchema(schemaName, Arrays.asList(new FlinkCDCTable(tableName, columns)));
    }
}
