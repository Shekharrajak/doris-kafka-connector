/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.doris.kafka.connector.share;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.*;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

public class DorisShareWorkerTest {
    private final ShareConsumer<String, String> consumer = mock(ShareConsumer.class);
    private final SinkTask task = mock(SinkTask.class);
    private final Map<String, String> config = new HashMap<>();
    private final AtomicBoolean stopping = new AtomicBoolean();
    private final DorisShareWorker worker = new DorisShareWorker(consumer, task, config, stopping);
    private final TopicPartition partition = new TopicPartition("events", 0);

    private ConsumerRecords<String, String> records(String value) {
        return new ConsumerRecords<>(
                Collections.singletonMap(
                        partition,
                        Arrays.asList(
                                new ConsumerRecord<>("events", 0, 101, "key", value),
                                new ConsumerRecord<>("events", 0, 100, "key", value))));
    }

    private Map<String, String> validConfig() throws Exception {
        Properties properties = new Properties();
        try (InputStream input =
                getClass().getResourceAsStream("/doris-connector-sink.properties")) {
            properties.load(input);
        }
        Map<String, String> result = new HashMap<>();
        properties.stringPropertyNames().forEach(k -> result.put(k, properties.getProperty(k)));
        result.remove("errors.deadletterqueue.topic.name");
        result.put("errors.tolerance", "none");
        result.put("doris.share.group.enabled", "true");
        result.put("consumer.bootstrap.servers", "localhost:9092");
        result.put("consumer.group.id", "share-events");
        return result;
    }

    @Test
    public void writesBeforeAcknowledgingAndPreservesOutOfOrderOffsets() {
        when(consumer.commitSync(any(Duration.class))).thenReturn(Collections.emptyMap());
        ConsumerRecords<String, String> input = records("{\"id\":1}");
        worker.process(input);
        ArgumentCaptor<Collection<SinkRecord>> captured = ArgumentCaptor.forClass(Collection.class);
        InOrder order = inOrder(task, consumer);
        order.verify(task).open(Collections.singleton(partition));
        order.verify(task).put(captured.capture());
        for (ConsumerRecord<String, String> record : input) {
            order.verify(consumer).acknowledge(record, AcknowledgeType.ACCEPT);
        }
        order.verify(consumer).commitSync(any(Duration.class));
        List<SinkRecord> output = new ArrayList<>(captured.getValue());
        assertEquals(101, output.get(0).kafkaOffset());
        assertEquals(100, output.get(1).kafkaOffset());
        assertEquals("{\"id\":1}", output.get(0).value());
    }

    @Test
    public void failedDorisWriteDoesNotAcknowledge() {
        doThrow(new RetriableException("Doris unavailable")).when(task).put(anyCollection());
        assertThrows(RetriableException.class, () -> worker.process(records("{}")));
        verifyNoInteractions(consumer);
    }

    @Test
    public void rejectsInvalidOrNullJsonWithoutWritingOrAcknowledging() {
        for (String value : Arrays.asList(null, "null", "[]", "bad json", "{} {}")) {
            assertThrows(DataException.class, () -> worker.process(records(value)));
        }
        verifyNoInteractions(task, consumer);
    }

    @Test
    public void acknowledgementFailureFailsTheWorker() {
        KafkaException failure = new KafkaException("ACK failed");
        when(consumer.commitSync(any(Duration.class)))
                .thenReturn(
                        Collections.singletonMap(
                                new TopicIdPartition(Uuid.randomUuid(), partition),
                                Optional.of(failure)));
        assertSame(
                failure, assertThrows(KafkaException.class, () -> worker.process(records("{}"))));
        verify(task).put(anyCollection());
    }

    @Test
    public void compactsMultilineJsonForLineBasedStreamLoad() {
        when(consumer.commitSync(any(Duration.class))).thenReturn(Collections.emptyMap());
        worker.process(records("{\n\"id\": 1\n}"));
        ArgumentCaptor<Collection<SinkRecord>> captured = ArgumentCaptor.forClass(Collection.class);
        verify(task).put(captured.capture());
        assertEquals("{\"id\":1}", captured.getValue().iterator().next().value());
    }

    @Test
    public void emptyPollDoesNotWriteOrCommit() {
        worker.process(ConsumerRecords.empty());
        verifyNoInteractions(task, consumer);
    }

    @Test
    public void opensNewPartitionsOnlyOnce() {
        when(consumer.commitSync(any(Duration.class))).thenReturn(Collections.emptyMap());
        worker.process(records("{}"));
        worker.process(records("{}"));
        verify(task, times(1)).open(Collections.singleton(partition));
    }

    @Test
    public void stopsTaskAndClosesConsumerAfterFailure() {
        config.put("topics", "events");
        when(consumer.poll(any(Duration.class))).thenReturn(records("{}"));
        doThrow(new RetriableException("failure")).when(task).put(anyCollection());
        assertThrows(RetriableException.class, worker::run);
        verify(task).stop();
        verify(consumer).close();
        verify(consumer, never()).commitSync(any(Duration.class));
    }

    @Test
    public void shutdownWakeupClosesResources() {
        config.put("topics", "events");
        when(consumer.poll(any(Duration.class)))
                .thenAnswer(
                        invocation -> {
                            stopping.set(true);
                            throw new WakeupException();
                        });
        worker.run();
        verify(task).stop();
        verify(consumer).close();
    }

    @Test
    public void requiresExplicitOptInAndConsumerIdentity() throws Exception {
        Map<String, String> config = validConfig();
        config.remove("doris.share.group.enabled");
        assertThrows(IllegalArgumentException.class, () -> DorisShareWorker.validate(config));
        config.put("doris.share.group.enabled", "true");
        config.remove("consumer.group.id");
        assertThrows(IllegalArgumentException.class, () -> DorisShareWorker.validate(config));
    }

    @Test
    public void validatesSinkAndPassesConsumerSecuritySettings() throws Exception {
        Map<String, String> config = validConfig();
        config.put("consumer.security.protocol", "SASL_SSL");
        DorisShareWorker.validate(config);
        Properties properties = DorisShareWorker.consumerProperties(config);
        assertEquals("explicit", properties.getProperty("share.acknowledgement.mode"));
        assertEquals("share-events", properties.getProperty("group.id"));
        assertEquals("SASL_SSL", properties.getProperty("security.protocol"));
        assertFalse(properties.containsKey("doris.urls"));
        config.put("consumer.share.acknowledgement.mode", "implicit");
        assertThrows(
                IllegalArgumentException.class, () -> DorisShareWorker.consumerProperties(config));
    }

    @Test
    public void rejectsUnsupportedRuntimeAndUnsafeSinkSettings() throws Exception {
        List<String[]> settings =
                Arrays.asList(
                        new String[] {"transforms", "route"},
                        new String[] {"errors.tolerance", "all"},
                        new String[] {"sink.properties.max_filter_ratio", "1"},
                        new String[] {"delivery.guarantee", "exactly_once"},
                        new String[] {"converter.mode", "debezium_ingestion"},
                        new String[] {"topics", "events,"});
        for (String[] setting : settings) {
            Map<String, String> config = validConfig();
            config.put(setting[0], setting[1]);
            assertThrows(IllegalArgumentException.class, () -> DorisShareWorker.validate(config));
        }
    }
}
