<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Doris-Kafka-Connector

[Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html) is a scalable and reliable tool for data transmission between Apache Kafka and other systems. Connectors can be defined Move large amounts of data in and out of Kafka.

Doris provides the Sink Connector plugin, which can write data from Kafka topics to Doris.
More information about usage, please visit [Doris Kafka Connector](https://doris.apache.org/docs/ecosystem/doris-kafka-connector)

## License

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

## How to Build

After running the following command, the jar package of Doris-Kafka-Connector will be generated in the dist directory.
```
sh build.sh
```
Note: Confluent archive zip file can be packaged through `sh build.sh --confluent`

## Code formatting
Doris-Kafka-Connector uses the AOSP style in google-java-format version 1.7 as the formatting style of the project code.

When you need to format your code, you have two formatting options:

- Execute `sh format.sh` under the project
- Execute `mvn spotless:apply` under the project

After executing the above formatting command, you can use `mvn spotless:check` to check whether the code format meets the requirements.

## TLS configuration

The connector supports opt-in one-way TLS for Doris HTTP and MySQL/JDBC connections. TLS is
disabled by default. When it is enabled, the connector does not probe plaintext endpoints or fall
back to plaintext after a TLS failure.

| Property | Default | Description |
| --- | --- | --- |
| `doris.enable.tls` | `false` | Enables TLS for Doris HTTP and MySQL/JDBC connections. |
| `doris.tls.ca-certificate-path` | empty | Worker-local PEM CA certificate or certificate-chain path. When empty, the JVM default trust store is used. |
| `doris.tls.skip-hostname-verification` | `false` | Disables hostname verification while retaining CA and certificate validation. |
| `doris.tls.excluded-protocols` | empty | Comma-separated protocols that remain plaintext. Supported values are `http` and `mysql`. |

Example:

```properties
doris.urls=doris-fe.example.com
doris.http.port=8040
doris.query.port=9030
doris.enable.tls=true
doris.tls.ca-certificate-path=/etc/kafka-connect/certs/doris-ca.pem
doris.tls.skip-hostname-verification=false
doris.tls.excluded-protocols=
```

`doris.urls` continues to contain host names only; do not add `http://`, `https://`, or a port. The
configured CA file must exist at the same local path on every Kafka Connect worker that may run the
task. Client certificates and private keys are not supported. Copy-into uploads to external object
storage use an independent JVM-system trust context and do not inherit a private Doris CA.
Plaintext Doris URLs are rejected when HTTP TLS is enabled. MySQL TLS requires the bundled
Connector/J 8 driver and never falls back to the legacy driver.

## Native Kafka Connect share worker

The experimental Kafka branch `feature/connect-share-group-sink` supports share
consumption inside the Connect worker. Build its API locally first:

```bash
./gradlew :connect:api:publishToMavenLocal -PskipSigning=true
```

Then build this connector with Java 17:

```bash
mvn -Pnative-connect-share -Dspotless.check.skip=true -DskipTests package
```

Install the jar from `target/native-connect-share` into the custom Connect runtime's
`plugin.path`. Configure the connector using `config/doris-native-share.properties`,
including both `consumer.type=share` and `doris.share.group.enabled=true`.
`DorisSinkConnector` selects `NativeDorisShareSinkTask`, a thin adapter around the
existing synchronous sink. Kafka Connect owns consumption, converters, SMTs and ACKs;
the standalone `DorisShareWorker` is not used in this path.

This requires the custom Connect API/runtime; stock Connect does not support the
new task contract. Keep `errors.tolerance=none`; DLQs and partition-offset operations
are unsupported. Delivery is at least once, including possible duplicate effects
between a successful Doris write and Kafka acknowledgement. The ordinary connector
build and consumer path remain the default.

## Opt-in Kafka share worker

The normal Kafka Connect worker owns its `KafkaConsumer`, calls `DorisSinkTask.put()`,
and commits offsets through `preCommit()`. The Doris connector does not select that consumer.

The standalone `DorisShareWorker` creates `KafkaShareConsumer` instances and reuses
`DorisSinkTask`. Build it with Java 17 and the `kafka-share` profile (Kafka 4.2.0);
the default build keeps Kafka 3.7.1. Use a broker with share groups enabled.

```bash
mvn -Pkafka-share -Dspotless.check.skip=true compile dependency:build-classpath -Dmdep.outputFile=share-classpath.txt
java -cp "target/kafka-share/classes:$(cat share-classpath.txt)" \
  org.apache.doris.kafka.connector.share.DorisShareWorker config/doris-share-worker.properties
```

The existing formatter requires Java 11; run `mvn spotless:check` with that JDK separately.

Edit the example properties for your topic, destination and credentials before starting.
`tasks.max` controls local reader threads; all use `consumer.group.id` and can share
partitions. `consumer.*` properties pass through to Kafka, including TLS/SASL settings.
Share-group starting position is configured at the broker/group, not through consumer offsets.

The worker requires `doris.share.group.enabled=true` and non-null JSON object values.
Each batch is loaded synchronously before individual records are ACCEPTed and their
acknowledgements committed. Delivery is **at least once**: a crash, expired record lock
or ACK failure after a successful Doris load can cause duplicate rows. Configure broker
retention, delivery-attempt limits and lock duration for the workload.

This is a standalone runtime, not a distributed Connect worker. It does not run Connect
converters, SMTs, DLQs, schema evolution or offset seeking. Sink and ACK failures stop the
process; restart with the same group ID for redelivery of unacknowledged records.
Setting the flag on a stock Connect worker only changes sink behavior; it does not
replace that worker's consumer. The share profile writes to `target/kafka-share`.

## Help
For additional help, please [file an issue in the repository](https://github.com/apache/doris-kafka-connector/issues) or raise a question in [Apache Doris Public Slack](https://apachedoriscommunity.slack.com/).
