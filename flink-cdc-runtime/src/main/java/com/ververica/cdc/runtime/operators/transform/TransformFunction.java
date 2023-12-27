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

package com.ververica.cdc.runtime.operators.transform;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import com.ververica.cdc.common.data.binary.BinaryRecordData;
import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.event.ColumnId;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** A map function that applies user-defined transform logics. */
public class TransformFunction extends RichMapFunction<Event, Event> {
    private final List<Tuple2<String, ColumnId>> transformRules;
    private transient List<Tuple2<Expression, ColumnId>> transformations;
    private transient JexlEngine jexlEngine;
    private transient List<DataType> dataTypes;
    private transient List<String> columnNames;

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder of {@link TransformFunction}. */
    public static class Builder {
        private final List<Tuple2<String, ColumnId>> transformRules = new ArrayList<>();

        public Builder addTransform(String columnTransform, ColumnId addBy) {
            transformRules.add(Tuple2.of(columnTransform, addBy));
            return this;
        }

        public TransformFunction build() {
            return new TransformFunction(transformRules);
        }
    }

    private TransformFunction(List<Tuple2<String, ColumnId>> transformRules) {
        this.transformRules = transformRules;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        jexlEngine = new JexlEngine();
        dataTypes = new ArrayList<>();
        columnNames = new ArrayList<>();
        transformations =
            transformRules.stream()
                .map(
                    tuple2 -> {
                        String expressionStr = tuple2.f0;
                        ColumnId addBy = tuple2.f1;
                        Expression expression = jexlEngine.createExpression(expressionStr);
                        return new Tuple2<>(expression, addBy);
                    })
                .collect(Collectors.toList());
        // todo: Change to retrieve from metadata
        columnNames.add("col1");
        columnNames.add("col2");
        dataTypes.add(DataTypes.STRING());
        dataTypes.add(DataTypes.STRING());

        for (Tuple2<String, ColumnId> route : transformRules) {
            ColumnId addBy = route.f1;
            columnNames.add(addBy.getColumnName());
            dataTypes.add(DataTypes.STRING());
        }
    }

    @Override
    public Event map(Event event) throws Exception {
        if (!(event instanceof DataChangeEvent)) {
            return event;
        }
        DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
        BinaryRecordData after = (BinaryRecordData) dataChangeEvent.after();
        if(after == null){
            return event;
        }
        List<Object> valueList = new ArrayList<>();

        JexlContext jexlContext = new MapContext();

        for(int i=0;i<after.getArity();i++){
            valueList.add(BinaryStringData.fromString(after.getString(i).toString()));
        }
        // todo: Change to retrieve from metadata
        jexlContext.set("col1", after.getString(0).toString());
        jexlContext.set("col2", after.getString(1).toString());

        for (Tuple2<Expression, ColumnId> route : transformations) {
            Expression expression = route.f0;
            Object evaluate = expression.evaluate(jexlContext);
            valueList.add(BinaryStringData.fromString(evaluate.toString()));
        }


        RowType rowType =
                RowType.of(
                        dataTypes.toArray(new DataType[dataTypes.size()]),
                        columnNames.toArray(new String[columnNames.size()]));
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
        BinaryRecordData data =
                generator.generate(
                    valueList.toArray(new Object[columnNames.size()])
                );

        return DataChangeEvent.setAfter(dataChangeEvent, data);
    }
}
