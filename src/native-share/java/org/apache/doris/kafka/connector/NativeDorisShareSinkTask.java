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

import java.util.Collection;
import java.util.Map;
import org.apache.doris.kafka.connector.cfg.DorisSinkConnectorConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ShareSinkTask;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class NativeDorisShareSinkTask extends ShareSinkTask {
    private final DorisSinkTask delegate;

    public NativeDorisShareSinkTask() {
        this(new DorisSinkTask());
    }

    NativeDorisShareSinkTask(DorisSinkTask delegate) {
        this.delegate = delegate;
    }

    @Override
    public void initialize(SinkTaskContext context) {
        super.initialize(context);
        delegate.initialize(context);
    }

    @Override
    public void start(Map<String, String> config) {
        if (!Boolean.parseBoolean(config.get(DorisSinkConnectorConfig.SHARE_GROUP_ENABLED))) {
            throw new ConnectException("Native share tasks require synchronous share-group writes");
        }
        delegate.start(config);
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        delegate.open(partitions);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        delegate.put(records);
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        delegate.close(partitions);
    }

    @Override
    public void stop() {
        delegate.stop();
    }

    @Override
    public String version() {
        return delegate.version();
    }
}
