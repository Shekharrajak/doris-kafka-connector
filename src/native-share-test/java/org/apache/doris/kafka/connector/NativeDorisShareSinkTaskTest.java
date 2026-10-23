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

package org.apache.doris.kafka.connector;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.InputStream;
import java.util.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Test;

public class NativeDorisShareSinkTaskTest {
    private final DorisSinkTask delegate = mock(DorisSinkTask.class);
    private final NativeDorisShareSinkTask task = new NativeDorisShareSinkTask(delegate);

    private Map<String, String> config() throws Exception {
        Properties properties = new Properties();
        try (InputStream input =
                getClass().getResourceAsStream("/doris-connector-sink.properties")) {
            properties.load(input);
        }
        Map<String, String> config = new HashMap<>();
        properties
                .stringPropertyNames()
                .forEach(key -> config.put(key, properties.getProperty(key)));
        config.put("consumer.type", "share");
        config.put("doris.share.group.enabled", "true");
        return config;
    }

    @Test
    public void selectsNativeTaskOnlyForShareConsumption() throws Exception {
        DorisSinkConnector connector = new DorisSinkConnector();
        Map<String, String> config = config();
        connector.start(config);
        assertEquals(NativeDorisShareSinkTask.class, connector.taskClass());
        config.put("consumer.type", "consumer");
        connector.start(config);
        assertEquals(DorisSinkTask.class, connector.taskClass());
    }

    @Test
    public void rejectsNativeShareWithoutSynchronousSinkMode() throws Exception {
        Map<String, String> config = config();
        config.remove("doris.share.group.enabled");
        DorisSinkConnector connector = new DorisSinkConnector();
        connector.start(config);
        assertThrows(ConnectException.class, connector::taskClass);
        assertThrows(ConnectException.class, () -> task.start(config));
        verifyNoInteractions(delegate);
    }

    @Test
    public void delegatesLifecycleAndDurableWrites() {
        SinkTaskContext context = mock(SinkTaskContext.class);
        Map<String, String> config = Map.of("doris.share.group.enabled", "true");
        Set<TopicPartition> partitions = Set.of(new TopicPartition("events", 0));
        List<SinkRecord> records =
                List.of(new SinkRecord("events", 0, null, null, null, "{}", 100));
        task.initialize(context);
        task.start(config);
        task.open(partitions);
        task.put(records);
        task.close(partitions);
        task.stop();
        verify(delegate).initialize(context);
        verify(delegate).start(config);
        verify(delegate).open(partitions);
        verify(delegate).put(records);
        verify(delegate).close(partitions);
        verify(delegate).stop();
    }

    @Test
    public void propagatesWriteFailuresToNativeWorker() {
        doThrow(new RetriableException("Doris failed")).when(delegate).put(anyCollection());
        assertThrows(RetriableException.class, () -> task.put(List.of()));
    }
}
