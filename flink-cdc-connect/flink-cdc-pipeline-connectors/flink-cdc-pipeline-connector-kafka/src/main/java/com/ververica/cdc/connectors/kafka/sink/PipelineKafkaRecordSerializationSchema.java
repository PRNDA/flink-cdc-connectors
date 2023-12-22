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

package com.ververica.cdc.connectors.kafka.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import com.ververica.cdc.common.event.ChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link KafkaRecordSerializationSchema} to serialize {@link Event}.
 *
 * <p>The topic to be sent is the string value of {@link TableId}.
 *
 * <p>the key of {@link ProducerRecord} is null as we don't need to upsert Kafka.
 */
public class PipelineKafkaRecordSerializationSchema
        implements KafkaRecordSerializationSchema<Event> {
    private final FlinkKafkaPartitioner<Event> partitioner;
    private final SerializationSchema<Event> valueSerialization;

    PipelineKafkaRecordSerializationSchema(
            @Nullable FlinkKafkaPartitioner<Event> partitioner,
            SerializationSchema<Event> valueSerialization) {
        this.partitioner = partitioner;
        this.valueSerialization = checkNotNull(valueSerialization);
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            Event event, KafkaSinkContext context, Long timestamp) {
        ChangeEvent changeEvent = (ChangeEvent) event;
        String topic = changeEvent.tableId().toString();
        final byte[] valueSerialized = valueSerialization.serialize(event);
        if (event instanceof SchemaChangeEvent) {
            // skip sending SchemaChangeEvent.
            return null;
        }
        return new ProducerRecord<>(
                topic,
                extractPartition(
                        changeEvent, valueSerialized, context.getPartitionsForTopic(topic)),
                null,
                valueSerialized);
    }

    @Override
    public void open(
            SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext)
            throws Exception {
        if (partitioner != null) {
            partitioner.open(
                    sinkContext.getParallelInstanceId(),
                    sinkContext.getNumberOfParallelInstances());
        }
        valueSerialization.open(context);
    }

    private Integer extractPartition(
            ChangeEvent changeEvent, byte[] valueSerialized, int[] partitions) {
        if (partitioner != null) {
            return partitioner.partition(
                    changeEvent,
                    null,
                    valueSerialized,
                    changeEvent.tableId().toString(),
                    partitions);
        }
        return null;
    }
}
