/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

package org.neo4j.connectors.kafka.testing.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.neo4j.connectors.kafka.testing.format.KafkaConverter

data class ConvertingKafkaConsumer(
    val keyConverter: KafkaConverter,
    val valueConverter: KafkaConverter,
    val kafkaConsumer: KafkaConsumer<*, *>
)

data class GenericRecord<K, V>(val key: K?, val value: V, val raw: ConsumerRecord<*, *>)
