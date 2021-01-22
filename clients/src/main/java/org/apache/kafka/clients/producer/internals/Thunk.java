/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * a collection of metadata which is exposed to users.
 */
class Thunk {
    final CompletableFuture<RecordMetadata> future;
    private final ProduceRequestResult result;
    private final long relativeOffset;
    private final long createTimestamp;
    final Long checksum;
    private final int serializedKeySize;
    private final int serializedValueSize;

    Thunk(CompletableFuture<RecordMetadata> future, ProduceRequestResult result, long relativeOffset, long createTimestamp,
          Long checksum, int serializedKeySize, int serializedValueSize) {
        this.future = Objects.requireNonNull(future);
        this.result = result;
        this.relativeOffset = relativeOffset;
        this.createTimestamp = createTimestamp;
        this.checksum = checksum;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
    }

    void done() {
        if (result.error() != null) future.completeExceptionally(result.error());
        else
            future.complete(new RecordMetadata(result.topicPartition(), result.baseOffset(),
                    relativeOffset, result.hasLogAppendTime() ? result.logAppendTime() : createTimestamp,
                    checksum, serializedKeySize, serializedValueSize));
    }
}