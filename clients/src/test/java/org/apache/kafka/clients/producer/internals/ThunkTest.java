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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.record.RecordBatch;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ThunkTest {

    private final TopicPartition topicPartition = new TopicPartition("test", 0);
    private final long baseOffset = 45;
    private final long relOffset = 5;

    @Test
    public void testTimeout() throws Exception {
        ProduceRequestResult request = new ProduceRequestResult(topicPartition);
        Thunk thunk = new Thunk(new CompletableFuture<>(), request, relOffset,
                RecordBatch.NO_TIMESTAMP, 0L, 0, 0);
        assertFalse(thunk.future.isDone(), "Request is not completed");
        assertThrows(TimeoutException.class, () -> thunk.future.get(5, TimeUnit.MILLISECONDS));

        request.set(baseOffset, RecordBatch.NO_TIMESTAMP, null);
        thunk.done();
        request.done();
        assertTrue(thunk.future.isDone());
        assertEquals(baseOffset + relOffset, thunk.future.get().offset());
    }

    @Test
    public void testException() throws Exception {
        CompletableFuture<RecordMetadata> future = asyncRequest(baseOffset, new CorruptRecordException(), 50L);
        assertThrows(ExecutionException.class, future::get);
    }

    @Test
    public void testMetadata() throws Exception {
        CompletableFuture<RecordMetadata> future = asyncRequest(baseOffset, null, 50L);
        assertEquals(baseOffset + relOffset, future.get().offset());
    }

    /* create a new request result that will be completed after the given timeout */
    public CompletableFuture<RecordMetadata> asyncRequest(final long baseOffset, final RuntimeException error, final long timeout) {
        final ProduceRequestResult request = new ProduceRequestResult(topicPartition);
        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        Thunk thunk = new Thunk(future, request,
                relOffset, RecordBatch.NO_TIMESTAMP, 0L, 0, 0);
        Thread thread = new Thread(() -> {
            try {
                Thread.sleep(timeout);
                request.set(baseOffset, RecordBatch.NO_TIMESTAMP, error);
                thunk.done();
                request.done();
            } catch (InterruptedException e) {
                // swallow
            }
        });
        thread.start();
        return future;
    }

}
