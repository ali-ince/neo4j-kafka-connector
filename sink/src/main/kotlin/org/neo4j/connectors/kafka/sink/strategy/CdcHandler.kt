/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.connectors.kafka.sink.strategy

import org.apache.kafka.connect.data.Struct
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.EntityEvent
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.EventType
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.connectors.kafka.data.StreamsTransactionEventExtensions.toChangeEvent
import org.neo4j.connectors.kafka.data.toChangeEvent
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.SinkStrategyHandler
import org.neo4j.connectors.kafka.sink.strategy.legacy.toStreamsSinkEntity
import org.neo4j.connectors.kafka.sink.strategy.legacy.toStreamsTransactionEvent
import org.neo4j.driver.Query
import org.slf4j.Logger
import org.slf4j.LoggerFactory

abstract class CdcHandler : SinkStrategyHandler {
  private val logger: Logger = LoggerFactory.getLogger(javaClass)
  private val buffer: MutableMap<Long, List<Pair<ChangeEvent, ChangeQuery>>> = mutableMapOf()
  private val comparator = ChangeComparator()

  data class MessageToEvent(val message: SinkMessage, val changeEvent: ChangeEvent)

  override fun handle(messages: Iterable<SinkMessage>): Iterable<Iterable<ChangeQuery>> {
    messages
        .onEach { logger.trace("received message: {}", it) }
        .map { MessageToEvent(it, it.toChangeEvent()) }
        .onEach { logger.trace("converted message: {} to {}", it.changeEvent.txId, it.changeEvent) }
        .groupBy(
            { it.changeEvent.txId },
            {
              Pair(
                  it.changeEvent,
                  ChangeQuery(
                      it.changeEvent.txId,
                      it.changeEvent.seq,
                      listOf(it.message),
                      when (val event = it.changeEvent.event) {
                        is NodeEvent ->
                            when (event.operation) {
                              EntityOperation.CREATE -> transformCreate(event)
                              EntityOperation.UPDATE -> transformUpdate(event)
                              EntityOperation.DELETE -> transformDelete(event)
                              else ->
                                  throw IllegalArgumentException(
                                      "unknown operation ${event.operation}")
                            }
                        is RelationshipEvent ->
                            when (event.operation) {
                              EntityOperation.CREATE -> transformCreate(event)
                              EntityOperation.UPDATE -> transformUpdate(event)
                              EntityOperation.DELETE -> transformDelete(event)
                              else ->
                                  throw IllegalArgumentException(
                                      "unknown operation ${event.operation}")
                            }
                        else ->
                            throw IllegalArgumentException(
                                "unsupported event type ${event.eventType}")
                      }))
            })
        .onEach { logger.trace("mapped messages: {} to {}", it.key, it.value) }
        .forEach { (txId, changes) -> { buffer.merge(txId, changes) { old, new -> old + new } } }

    // only process completely seen transactions
    val txIds = buffer.keys.sorted().dropLast(1)

    return txIds
        .map { txId -> buffer.remove(txId)!! }
        .map { changes -> changes.sortedWith(comparator) }
        .map { changes -> changes.map { it.second } }
  }

  protected abstract fun transformCreate(event: NodeEvent): Query

  protected abstract fun transformUpdate(event: NodeEvent): Query

  protected abstract fun transformDelete(event: NodeEvent): Query

  protected abstract fun transformCreate(event: RelationshipEvent): Query

  protected abstract fun transformUpdate(event: RelationshipEvent): Query

  protected abstract fun transformDelete(event: RelationshipEvent): Query
}

internal fun SinkMessage.toChangeEvent(): ChangeEvent =
    when {
      this.isCdcMessage -> parseCdcChangeEvent(this)
      else -> parseStreamsChangeEvent(this)
    }

internal fun parseCdcChangeEvent(message: SinkMessage): ChangeEvent =
    when (val value = message.value) {
      is Struct -> value.toChangeEvent()
      else ->
          throw IllegalArgumentException(
              "unexpected message value type ${value?.javaClass?.name} in $message")
    }

internal fun parseStreamsChangeEvent(message: SinkMessage): ChangeEvent {
  val event =
      message.record.toStreamsSinkEntity().toStreamsTransactionEvent { _ -> true }
          ?: throw IllegalArgumentException("unsupported change event message in $message")

  return event.toChangeEvent()
}

internal class ChangeComparator : Comparator<Pair<ChangeEvent, ChangeQuery>> {
  override fun compare(
      o1: Pair<ChangeEvent, ChangeQuery>,
      o2: Pair<ChangeEvent, ChangeQuery>
  ): Int {
    val c1 = o1.first.event as EntityEvent<*>?
    val c2 = o2.first.event as EntityEvent<*>?

    return when {
      c1 == null && c2 == null -> 0
      c1 != null && c2 == null -> 1
      c1 == null && c2 != null -> -1
      else ->
          when {
            c1!!.operation == EntityOperation.CREATE && c2!!.operation == EntityOperation.CREATE ->
                if (c1.eventType == EventType.NODE && c2.eventType == EventType.RELATIONSHIP) 1
                else if (c1.eventType == EventType.RELATIONSHIP && c2.eventType == EventType.NODE)
                    -1
                else 0
            c1.operation == EntityOperation.CREATE && c2!!.operation == EntityOperation.UPDATE -> -1
            c1.operation == EntityOperation.CREATE && c2!!.operation == EntityOperation.DELETE -> -1
            c1.operation == EntityOperation.UPDATE && c2!!.operation == EntityOperation.UPDATE ->
                if (c1.eventType == EventType.NODE && c2.eventType == EventType.RELATIONSHIP) 1
                else if (c1.eventType == EventType.RELATIONSHIP && c2.eventType == EventType.NODE)
                    -1
                else 0
            c1.operation == EntityOperation.UPDATE && c2!!.operation == EntityOperation.CREATE -> 1
            c1.operation == EntityOperation.UPDATE && c2!!.operation == EntityOperation.DELETE -> -1
            c1.operation == EntityOperation.DELETE && c2!!.operation == EntityOperation.DELETE ->
                if (c1.eventType == EventType.NODE && c2.eventType == EventType.RELATIONSHIP) -1
                else if (c1.eventType == EventType.RELATIONSHIP && c2.eventType == EventType.NODE) 1
                else 0
            c1.operation == EntityOperation.DELETE && c2!!.operation == EntityOperation.CREATE -> 1
            c1.operation == EntityOperation.DELETE && c2!!.operation == EntityOperation.UPDATE -> 1
            else -> 0
          }
    }
  }
}
