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
package streams.utils

object StreamsUtils {

  @JvmStatic val UNWIND: String = "UNWIND \$events AS event"

  @JvmStatic val WITH_EVENT_FROM: String = "WITH event, from"

  @JvmStatic val STREAMS_CONFIG_PREFIX = "streams."

  @JvmStatic val STREAMS_SINK_TOPIC_PREFIX = "sink.topic.cypher."

  @JvmStatic val LEADER = "LEADER"

  @JvmStatic val SYSTEM_DATABASE_NAME = "system"

  fun <T> ignoreExceptions(action: () -> T, vararg toIgnore: Class<out Throwable>): T? {
    return try {
      action()
    } catch (e: Throwable) {
      if (toIgnore.isEmpty()) {
        return null
      }
      return if (toIgnore.any { it.isInstance(e) }) {
        null
      } else {
        throw e
      }
    }
  }

  fun closeSafetely(closeable: AutoCloseable, onError: (Throwable) -> Unit = {}) =
    try {
      closeable.close()
    } catch (e: Throwable) {
      onError(e)
    }
}
