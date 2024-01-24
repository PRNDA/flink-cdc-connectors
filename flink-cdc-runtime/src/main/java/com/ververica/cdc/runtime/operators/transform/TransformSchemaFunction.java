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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import com.ververica.cdc.common.data.binary.BinaryRecordData;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.schema.Selectors;
import com.ververica.cdc.common.utils.SchemaUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** A schema process function that applies user-defined transform logics. */
public class TransformSchemaFunction extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event> {

    private final List<Tuple2<String, String>> transformRules;
    private transient List<Tuple2<Selectors, Projector>> transforms;

    /** keep the relationship of TableId and table information. */
    private final Map<TableId, TableInfo> tableInfoMap;

    public static TransformSchemaFunction.Builder newBuilder() {
        return new TransformSchemaFunction.Builder();
    }

    /** Builder of {@link TransformSchemaFunction}. */
    public static class Builder {
        private final List<Tuple2<String, String>> transformRules = new ArrayList<>();

        public TransformSchemaFunction.Builder addTransform(
                String tableInclusions, String projection) {
            transformRules.add(Tuple2.of(tableInclusions, projection));
            return this;
        }

        public TransformSchemaFunction build() {
            return new TransformSchemaFunction(transformRules);
        }
    }

    private TransformSchemaFunction(List<Tuple2<String, String>> transformRules) {
        this.transformRules = transformRules;
        this.tableInfoMap = new ConcurrentHashMap<>();
    }

    @Override
    public void open() throws Exception {
        super.open();
        transforms =
                transformRules.stream()
                        .map(
                                tuple2 -> {
                                    String tableInclusions = tuple2.f0;
                                    String projection = tuple2.f1;

                                    Selectors selectors =
                                            new Selectors.SelectorsBuilder()
                                                    .includeTables(tableInclusions)
                                                    .build();
                                    return new Tuple2<>(
                                            selectors, Projector.generateProjector(projection));
                                })
                        .collect(Collectors.toList());
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();
        if (event instanceof SchemaChangeEvent) {
            event = cacheLatestSchema((SchemaChangeEvent) event);
            output.collect(new StreamRecord<>(event));
        } else if (event instanceof DataChangeEvent) {
            Optional<DataChangeEvent> dataChangeEventOptional =
                    applyDataChangeEvent(((DataChangeEvent) event));
            if (dataChangeEventOptional.isPresent()) {
                output.collect(new StreamRecord<>(dataChangeEventOptional.get()));
            }
        }
    }

    private SchemaChangeEvent cacheLatestSchema(SchemaChangeEvent event) {
        TableId tableId = event.tableId();
        Schema newSchema;
        if (event instanceof CreateTableEvent) {
            newSchema = ((CreateTableEvent) event).getSchema();
            event = transformCreateTableEvent((CreateTableEvent) event);
        } else {
            TableInfo tableInfo = tableInfoMap.get(tableId);
            if (tableInfo == null) {
                throw new RuntimeException("Schema of " + tableId + " is not existed.");
            }
            newSchema = SchemaUtils.applySchemaChangeEvent(tableInfo.getSchema(), event);
            applyNewSchema(tableId, newSchema);
        }
        tableInfoMap.put(tableId, TableInfo.of(newSchema));
        return event;
    }

    private CreateTableEvent transformCreateTableEvent(CreateTableEvent createTableEvent) {
        TableId tableId = createTableEvent.tableId();
        tableInfoMap.put(tableId, TableInfo.of(createTableEvent.getSchema()));
        for (Tuple2<Selectors, Projector> route : transforms) {
            Selectors selectors = route.f0;
            if (selectors.isMatch(tableId)) {
                Projector projector = route.f1;
                // update the columns of projection and add the column of projection into Schema
                return projector.applyCreateTableEvent(createTableEvent);
            }
        }
        return createTableEvent;
    }

    private void applyNewSchema(TableId tableId, Schema schema) {
        tableInfoMap.put(tableId, TableInfo.of(schema));
        for (Tuple2<Selectors, Projector> route : transforms) {
            Selectors selectors = route.f0;
            if (selectors.isMatch(tableId)) {
                Projector projector = route.f1;
                // update the columns of projection
                projector.applyNewSchema(schema);
            }
        }
    }

    private Optional<DataChangeEvent> applyDataChangeEvent(DataChangeEvent dataChangeEvent) {
        Optional<DataChangeEvent> dataChangeEventOptional = Optional.of(dataChangeEvent);
        TableId tableId = dataChangeEvent.tableId();

        for (Tuple2<Selectors, Projector> transform : transforms) {
            Selectors selectors = transform.f0;
            if (selectors.isMatch(tableId)) {
                Projector projector = transform.f1;
                if (projector != null && projector.isVaild()) {
                    dataChangeEventOptional =
                            applyProjection(projector, dataChangeEventOptional.get());
                }
            }
        }
        return dataChangeEventOptional;
    }

    private Optional applyProjection(Projector projector, DataChangeEvent dataChangeEvent) {
        BinaryRecordData before = (BinaryRecordData) dataChangeEvent.before();
        BinaryRecordData after = (BinaryRecordData) dataChangeEvent.after();
        if (before != null) {
            BinaryRecordData data =
                    projector.recordFillDataField(
                            before, tableInfoMap.get(dataChangeEvent.tableId()));
            dataChangeEvent = DataChangeEvent.resetBefore(dataChangeEvent, data);
        }
        if (after != null) {
            BinaryRecordData data =
                    projector.recordFillDataField(
                            after, tableInfoMap.get(dataChangeEvent.tableId()));
            dataChangeEvent = DataChangeEvent.resetAfter(dataChangeEvent, data);
        }
        return Optional.of(dataChangeEvent);
    }
}
