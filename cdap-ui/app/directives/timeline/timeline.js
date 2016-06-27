/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

function link (scope, element) {

      let timelineData = [
        {
          time: '2016-03-04 16:28:40',
          level: 'INFO',
          source: 'leader-election-election-metrics-processor-part-0',
          message: {
            content: 'Got exception publishing audit message AuditMessage{version=1, time=1465850244499, entityId=dataset:default._kafkaOffset, user=\'\', type=ACCESS, payload=AccessPayload{accessType=UNKNOWN, accessor=program_run:default._Tracker.flow.AuditLogFlow.a1e96ae1-31a6-11e6-8c41-e285682178e6} AuditPayload{}}.',
            stackTrace: 'java.util.concurrent.ExecutionException: java.lang.IllegalStateException: No kafka producer available.\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.getValue(AbstractFuture.java:294) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.get(AbstractFuture.java:281) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture.get(AbstractFuture.java:116) ~[guava-13.0.1.jar:na]\n\tat co.cask.cdap.data2.audit.KafkaAuditPublisher.publish(KafkaAuditPublisher.java:72) ~[classes/:na]\n\tat co.cask.cdap.data2.audit.AuditPublishers.publishAccess(AuditPublishers.java:90) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.doWriteLineage(LineageWriterDatasetFramework.java:187) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.writeLineage(LineageWriterDatasetFramework.java:169) [classes/:na]\n\tat co.cask.cdap.data.dataset.SystemDatasetInstantiator.writeLineage(SystemDatasetInstantiator.java:108) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:143) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:127) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache.getDataset(SingleThreadDatasetCache.java:170) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:150) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:126) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:179) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:174) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:168) [classes/:na]\n\tat co.cask.tracker.AuditLogConsumer.configureKafka(AuditLogConsumer.java:92) [unpacked/:na]\n\tat co.cask.cdap.kafka.flow.KafkaConsumerFlowlet.initialize(KafkaConsumerFlowlet.java:101) [cdap-kafka-flow-core-0.9.0.jar:na]\n\tat co.cask.cdap.kafka.flow.Kafka08ConsumerFlowlet.initialize(Kafka08ConsumerFlowlet.java:97) [cdap-kafka-flow-compat-0.8-0.9.0.jar:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService$1.apply(FlowletRuntimeService.java:115) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:92) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeOnce(DynamicTransactionExecutor.java:125) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeWithRetry(DynamicTransactionExecutor.java:104) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:61) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.initFlowlet(FlowletRuntimeService.java:109) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.startUp(FlowletRuntimeService.java:71) [classes/:na]\n\tat com.google.common.util.concurrent.AbstractIdleService$1$1.run(AbstractIdleService.java:43) [guava-13.0.1.jar:na]\n\tat java.lang.Thread.run(Thread.java:745) [na:1.7.0_80]'
          }
        },
        {
          time: '2016-03-04 16:28:43',
          level: 'ERROR',
          source: 'leader-election-election-metrics-processor-part-0',
          message: {
            content: 'Some Other log data that is irrelevant for this demo.'
          }
        },
        {
          time: '2016-03-04 16:28:45',
          level: 'WARN',
          source: 'leader-election-election-metrics-processor-part-0',
          message: {
            content: 'Got exception publishing audit message AuditMessage{version=1, time=1465850244499, entityId=dataset:default._kafkaOffset, user=\'\', type=ACCESS, payload=AccessPayload{accessType=UNKNOWN, accessor=program_run:default._Tracker.flow.AuditLogFlow.a1e96ae1-31a6-11e6-8c41-e285682178e6} AuditPayload{}}.',
            stackTrace: 'java.util.concurrent.ExecutionException: java.lang.IllegalStateException: No kafka producer available.\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.getValue(AbstractFuture.java:294) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.get(AbstractFuture.java:281) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture.get(AbstractFuture.java:116) ~[guava-13.0.1.jar:na]\n\tat co.cask.cdap.data2.audit.KafkaAuditPublisher.publish(KafkaAuditPublisher.java:72) ~[classes/:na]\n\tat co.cask.cdap.data2.audit.AuditPublishers.publishAccess(AuditPublishers.java:90) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.doWriteLineage(LineageWriterDatasetFramework.java:187) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.writeLineage(LineageWriterDatasetFramework.java:169) [classes/:na]\n\tat co.cask.cdap.data.dataset.SystemDatasetInstantiator.writeLineage(SystemDatasetInstantiator.java:108) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:143) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:127) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache.getDataset(SingleThreadDatasetCache.java:170) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:150) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:126) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:179) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:174) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:168) [classes/:na]\n\tat co.cask.tracker.AuditLogConsumer.configureKafka(AuditLogConsumer.java:92) [unpacked/:na]\n\tat co.cask.cdap.kafka.flow.KafkaConsumerFlowlet.initialize(KafkaConsumerFlowlet.java:101) [cdap-kafka-flow-core-0.9.0.jar:na]\n\tat co.cask.cdap.kafka.flow.Kafka08ConsumerFlowlet.initialize(Kafka08ConsumerFlowlet.java:97) [cdap-kafka-flow-compat-0.8-0.9.0.jar:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService$1.apply(FlowletRuntimeService.java:115) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:92) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeOnce(DynamicTransactionExecutor.java:125) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeWithRetry(DynamicTransactionExecutor.java:104) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:61) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.initFlowlet(FlowletRuntimeService.java:109) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.startUp(FlowletRuntimeService.java:71) [classes/:na]\n\tat com.google.common.util.concurrent.AbstractIdleService$1$1.run(AbstractIdleService.java:43) [guava-13.0.1.jar:na]\n\tat java.lang.Thread.run(Thread.java:745) [na:1.7.0_80]'
          }
        },
        {
          time: '2016-03-04 16:28:47',
          level: 'DEBUG',
          source: 'leader-election-election-metrics-processor-part-0',
          message: {
            content: 'Got exception publishing audit message AuditMessage{version=1, time=1465850244499, entityId=dataset:default._kafkaOffset, user=\'\', type=ACCESS, payload=AccessPayload{accessType=UNKNOWN, accessor=program_run:default._Tracker.flow.AuditLogFlow.a1e96ae1-31a6-11e6-8c41-e285682178e6} AuditPayload{}}.',
            stackTrace: 'java.util.concurrent.ExecutionException: java.lang.IllegalStateException: No kafka producer available.\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.getValue(AbstractFuture.java:294) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.get(AbstractFuture.java:281) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture.get(AbstractFuture.java:116) ~[guava-13.0.1.jar:na]\n\tat co.cask.cdap.data2.audit.KafkaAuditPublisher.publish(KafkaAuditPublisher.java:72) ~[classes/:na]\n\tat co.cask.cdap.data2.audit.AuditPublishers.publishAccess(AuditPublishers.java:90) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.doWriteLineage(LineageWriterDatasetFramework.java:187) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.writeLineage(LineageWriterDatasetFramework.java:169) [classes/:na]\n\tat co.cask.cdap.data.dataset.SystemDatasetInstantiator.writeLineage(SystemDatasetInstantiator.java:108) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:143) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:127) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache.getDataset(SingleThreadDatasetCache.java:170) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:150) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:126) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:179) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:174) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:168) [classes/:na]\n\tat co.cask.tracker.AuditLogConsumer.configureKafka(AuditLogConsumer.java:92) [unpacked/:na]\n\tat co.cask.cdap.kafka.flow.KafkaConsumerFlowlet.initialize(KafkaConsumerFlowlet.java:101) [cdap-kafka-flow-core-0.9.0.jar:na]\n\tat co.cask.cdap.kafka.flow.Kafka08ConsumerFlowlet.initialize(Kafka08ConsumerFlowlet.java:97) [cdap-kafka-flow-compat-0.8-0.9.0.jar:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService$1.apply(FlowletRuntimeService.java:115) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:92) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeOnce(DynamicTransactionExecutor.java:125) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeWithRetry(DynamicTransactionExecutor.java:104) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:61) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.initFlowlet(FlowletRuntimeService.java:109) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.startUp(FlowletRuntimeService.java:71) [classes/:na]\n\tat com.google.common.util.concurrent.AbstractIdleService$1$1.run(AbstractIdleService.java:43) [guava-13.0.1.jar:na]\n\tat java.lang.Thread.run(Thread.java:745) [na:1.7.0_80]'
          }
        },
        {
          time: '2016-03-04 16:28:51',
          level: 'TRACE',
          source: 'leader-election-election-metrics-processor-part-0',
          message: {
            content: 'Some Other log data that is irrelevant for this demo.'
          }
        },
        {
          time: '2016-03-04 16:28:57',
          level: 'INFO',
          source: 'leader-election-election-metrics-processor-part-0',
          message: {
            content: 'Got exception publishing audit message AuditMessage{version=1, time=1465850244499, entityId=dataset:default._kafkaOffset, user=\'\', type=ACCESS, payload=AccessPayload{accessType=UNKNOWN, accessor=program_run:default._Tracker.flow.AuditLogFlow.a1e96ae1-31a6-11e6-8c41-e285682178e6} AuditPayload{}}.',
            stackTrace: 'java.util.concurrent.ExecutionException: java.lang.IllegalStateException: No kafka producer available.\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.getValue(AbstractFuture.java:294) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.get(AbstractFuture.java:281) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture.get(AbstractFuture.java:116) ~[guava-13.0.1.jar:na]\n\tat co.cask.cdap.data2.audit.KafkaAuditPublisher.publish(KafkaAuditPublisher.java:72) ~[classes/:na]\n\tat co.cask.cdap.data2.audit.AuditPublishers.publishAccess(AuditPublishers.java:90) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.doWriteLineage(LineageWriterDatasetFramework.java:187) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.writeLineage(LineageWriterDatasetFramework.java:169) [classes/:na]\n\tat co.cask.cdap.data.dataset.SystemDatasetInstantiator.writeLineage(SystemDatasetInstantiator.java:108) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:143) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:127) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache.getDataset(SingleThreadDatasetCache.java:170) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:150) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:126) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:179) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:174) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:168) [classes/:na]\n\tat co.cask.tracker.AuditLogConsumer.configureKafka(AuditLogConsumer.java:92) [unpacked/:na]\n\tat co.cask.cdap.kafka.flow.KafkaConsumerFlowlet.initialize(KafkaConsumerFlowlet.java:101) [cdap-kafka-flow-core-0.9.0.jar:na]\n\tat co.cask.cdap.kafka.flow.Kafka08ConsumerFlowlet.initialize(Kafka08ConsumerFlowlet.java:97) [cdap-kafka-flow-compat-0.8-0.9.0.jar:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService$1.apply(FlowletRuntimeService.java:115) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:92) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeOnce(DynamicTransactionExecutor.java:125) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeWithRetry(DynamicTransactionExecutor.java:104) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:61) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.initFlowlet(FlowletRuntimeService.java:109) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.startUp(FlowletRuntimeService.java:71) [classes/:na]\n\tat com.google.common.util.concurrent.AbstractIdleService$1$1.run(AbstractIdleService.java:43) [guava-13.0.1.jar:na]\n\tat java.lang.Thread.run(Thread.java:745) [na:1.7.0_80]'
          }
        },
        {
          time: '2016-03-04 16:29:00',
          level: 'ERROR',
          source: 'leader-election-election-metrics-processor-part-0',
          message: {
            content: 'Some Other log data that is irrelevant for this demo.'
          }
        },
        {
          time: '2016-03-04 16:29:15',
          level: 'WARN',
          source: 'leader-election-election-metrics-processor-part-0',
          message: {
            content: 'Got exception publishing audit message AuditMessage{version=1, time=1465850244499, entityId=dataset:default._kafkaOffset, user=\'\', type=ACCESS, payload=AccessPayload{accessType=UNKNOWN, accessor=program_run:default._Tracker.flow.AuditLogFlow.a1e96ae1-31a6-11e6-8c41-e285682178e6} AuditPayload{}}.',
            stackTrace: 'java.util.concurrent.ExecutionException: java.lang.IllegalStateException: No kafka producer available.\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.getValue(AbstractFuture.java:294) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.get(AbstractFuture.java:281) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture.get(AbstractFuture.java:116) ~[guava-13.0.1.jar:na]\n\tat co.cask.cdap.data2.audit.KafkaAuditPublisher.publish(KafkaAuditPublisher.java:72) ~[classes/:na]\n\tat co.cask.cdap.data2.audit.AuditPublishers.publishAccess(AuditPublishers.java:90) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.doWriteLineage(LineageWriterDatasetFramework.java:187) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.writeLineage(LineageWriterDatasetFramework.java:169) [classes/:na]\n\tat co.cask.cdap.data.dataset.SystemDatasetInstantiator.writeLineage(SystemDatasetInstantiator.java:108) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:143) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:127) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache.getDataset(SingleThreadDatasetCache.java:170) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:150) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:126) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:179) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:174) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:168) [classes/:na]\n\tat co.cask.tracker.AuditLogConsumer.configureKafka(AuditLogConsumer.java:92) [unpacked/:na]\n\tat co.cask.cdap.kafka.flow.KafkaConsumerFlowlet.initialize(KafkaConsumerFlowlet.java:101) [cdap-kafka-flow-core-0.9.0.jar:na]\n\tat co.cask.cdap.kafka.flow.Kafka08ConsumerFlowlet.initialize(Kafka08ConsumerFlowlet.java:97) [cdap-kafka-flow-compat-0.8-0.9.0.jar:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService$1.apply(FlowletRuntimeService.java:115) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:92) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeOnce(DynamicTransactionExecutor.java:125) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeWithRetry(DynamicTransactionExecutor.java:104) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:61) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.initFlowlet(FlowletRuntimeService.java:109) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.startUp(FlowletRuntimeService.java:71) [classes/:na]\n\tat com.google.common.util.concurrent.AbstractIdleService$1$1.run(AbstractIdleService.java:43) [guava-13.0.1.jar:na]\n\tat java.lang.Thread.run(Thread.java:745) [na:1.7.0_80]'
          }
        },
        {
          time: '2016-03-04 16:29:35',
          level: 'DEBUG',
          source: 'leader-election-election-metrics-processor-part-0',
          message: {
            content: 'Got exception publishing audit message AuditMessage{version=1, time=1465850244499, entityId=dataset:default._kafkaOffset, user=\'\', type=ACCESS, payload=AccessPayload{accessType=UNKNOWN, accessor=program_run:default._Tracker.flow.AuditLogFlow.a1e96ae1-31a6-11e6-8c41-e285682178e6} AuditPayload{}}.',
            stackTrace: 'java.util.concurrent.ExecutionException: java.lang.IllegalStateException: No kafka producer available.\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.getValue(AbstractFuture.java:294) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.get(AbstractFuture.java:281) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture.get(AbstractFuture.java:116) ~[guava-13.0.1.jar:na]\n\tat co.cask.cdap.data2.audit.KafkaAuditPublisher.publish(KafkaAuditPublisher.java:72) ~[classes/:na]\n\tat co.cask.cdap.data2.audit.AuditPublishers.publishAccess(AuditPublishers.java:90) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.doWriteLineage(LineageWriterDatasetFramework.java:187) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.writeLineage(LineageWriterDatasetFramework.java:169) [classes/:na]\n\tat co.cask.cdap.data.dataset.SystemDatasetInstantiator.writeLineage(SystemDatasetInstantiator.java:108) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:143) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:127) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache.getDataset(SingleThreadDatasetCache.java:170) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:150) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:126) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:179) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:174) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:168) [classes/:na]\n\tat co.cask.tracker.AuditLogConsumer.configureKafka(AuditLogConsumer.java:92) [unpacked/:na]\n\tat co.cask.cdap.kafka.flow.KafkaConsumerFlowlet.initialize(KafkaConsumerFlowlet.java:101) [cdap-kafka-flow-core-0.9.0.jar:na]\n\tat co.cask.cdap.kafka.flow.Kafka08ConsumerFlowlet.initialize(Kafka08ConsumerFlowlet.java:97) [cdap-kafka-flow-compat-0.8-0.9.0.jar:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService$1.apply(FlowletRuntimeService.java:115) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:92) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeOnce(DynamicTransactionExecutor.java:125) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeWithRetry(DynamicTransactionExecutor.java:104) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:61) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.initFlowlet(FlowletRuntimeService.java:109) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.startUp(FlowletRuntimeService.java:71) [classes/:na]\n\tat com.google.common.util.concurrent.AbstractIdleService$1$1.run(AbstractIdleService.java:43) [guava-13.0.1.jar:na]\n\tat java.lang.Thread.run(Thread.java:745) [na:1.7.0_80]'
          }
        },
        {
          time: '2016-03-04 16:29:46',
          level: 'TRACE',
          source: 'leader-election-election-metrics-processor-part-0',
          message: {
            content: 'Some Other log data that is irrelevant for this demo.'
          }
        },
        {
          time: '2016-03-04 16:29:55',
          level: 'INFO',
          source: 'leader-election-election-metrics-processor-part-0',
          message: {
            content: 'Got exception publishing audit message AuditMessage{version=1, time=1465850244499, entityId=dataset:default._kafkaOffset, user=\'\', type=ACCESS, payload=AccessPayload{accessType=UNKNOWN, accessor=program_run:default._Tracker.flow.AuditLogFlow.a1e96ae1-31a6-11e6-8c41-e285682178e6} AuditPayload{}}.',
            stackTrace: 'java.util.concurrent.ExecutionException: java.lang.IllegalStateException: No kafka producer available.\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.getValue(AbstractFuture.java:294) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.get(AbstractFuture.java:281) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture.get(AbstractFuture.java:116) ~[guava-13.0.1.jar:na]\n\tat co.cask.cdap.data2.audit.KafkaAuditPublisher.publish(KafkaAuditPublisher.java:72) ~[classes/:na]\n\tat co.cask.cdap.data2.audit.AuditPublishers.publishAccess(AuditPublishers.java:90) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.doWriteLineage(LineageWriterDatasetFramework.java:187) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.writeLineage(LineageWriterDatasetFramework.java:169) [classes/:na]\n\tat co.cask.cdap.data.dataset.SystemDatasetInstantiator.writeLineage(SystemDatasetInstantiator.java:108) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:143) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:127) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache.getDataset(SingleThreadDatasetCache.java:170) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:150) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:126) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:179) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:174) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:168) [classes/:na]\n\tat co.cask.tracker.AuditLogConsumer.configureKafka(AuditLogConsumer.java:92) [unpacked/:na]\n\tat co.cask.cdap.kafka.flow.KafkaConsumerFlowlet.initialize(KafkaConsumerFlowlet.java:101) [cdap-kafka-flow-core-0.9.0.jar:na]\n\tat co.cask.cdap.kafka.flow.Kafka08ConsumerFlowlet.initialize(Kafka08ConsumerFlowlet.java:97) [cdap-kafka-flow-compat-0.8-0.9.0.jar:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService$1.apply(FlowletRuntimeService.java:115) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:92) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeOnce(DynamicTransactionExecutor.java:125) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeWithRetry(DynamicTransactionExecutor.java:104) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:61) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.initFlowlet(FlowletRuntimeService.java:109) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.startUp(FlowletRuntimeService.java:71) [classes/:na]\n\tat com.google.common.util.concurrent.AbstractIdleService$1$1.run(AbstractIdleService.java:43) [guava-13.0.1.jar:na]\n\tat java.lang.Thread.run(Thread.java:745) [na:1.7.0_80]'
          }
        },
      ];

      //Convert all of the dates to the standard date format
      timelineData.forEach(function(element, index){
        timelineData[index].time = new Date(timelineData[index].time);
      });

      //Contains only the 'WARN' and 'ERROR' events
      let filteredEvents = timelineData.filter(function(obj) {
        if(obj.level === 'WARN' || obj.level === 'ERROR'){
          return true;
        }
        return false;
      });

      let minDate = filteredEvents[0],
          maxDate = filteredEvents[filteredEvents.length-1];

      //Global Variables
      var width = element.parent()[0].offsetWidth;
      var height = 50;
      var paddingLeft = 15;
      var paddingRight = 15;
      var maxRange = width - paddingLeft - paddingRight;
      var sliderLimit = maxRange + 24;

      //Plot function call
      plot();

      function plot() {

        // -----------------Define SVG and Plot Circles-------------------------- //
        var svg = d3.select('.timeline-log-chart')
                    .append('svg')
                    .attr('width', width)
                    .attr('height', height);

        //Set the Range and Domain
        //var xScale = d3.time.scale().range([0, (maxRange)]);
        var xScale = d3.time.scale().range([0, (width)]);
        xScale.domain(d3.extent(timelineData, function(d) {
          return d.time;
        }));

        //Define the axes and ticks
        var xAxis = d3.svg.axis().scale(xScale)
            .orient('bottom')
            .innerTickSize(-40)
            .tickPadding(7)
            .ticks(8);

        //Generate circles from the filtered events
        let circles = svg.selectAll('circle')
          .data(filteredEvents)
          .enter()
          .append('circle');

        let xScale = d3.time.scale()
                             .domain([minDate, maxDate])
                             .nice(d3.time.minute)
                             .range([0,width]);

        circles.attr('cx', function(d) {
          let xVal = Math.floor(xScale(d.time));
          if(timelineStack[xVal] === undefined){
            timelineStack[xVal] = 0;
          } else {
            timelineStack[xVal]++;
          }
          return xScale(d.time);
        })
        .attr('cy', function(d) {
          let numDots = timelineStack[Math.floor(xScale(d.time))]--;
          return height-height/2.5 - (numDots * 6);
        })
        .attr('r', 2)
        .attr('class', function(d) {
          if(d.level === 'ERROR'){
            return 'red-circle';
          }
          else if(d.level === 'WARN'){
            return 'yellow-circle';
          } else {
            return 'other-circle';
          }
        });

        // -------------------------Build Brush / Sliders------------------------- //

        //X-Axis
        svg.append('g')
          .attr('class', 'xaxisBottom')
          .attr('transform', 'translate(' + ( (paddingLeft + paddingRight) / 2) + ',' + (height - 20) + ')')
          .call(xAxis);

        //Left Slider Implementation
        var leftVal = 0;//10;

        function leftBrushed() {
          if(d3.event.sourceEvent) {
            var v = xScale.invert(d3.mouse(this)[0]);
            var index = d3.mouse(this)[0];

            if(v !== leftVal){
              leftVal = v;
            }

            update(index);
          }
        }

        var brush = d3.svg.brush()
            .x(xScale)
            .extent([0,0])
            .on('brush', leftBrushed);

        //Create the 3 bars used to represent the slider
        var sliderBar = svg.append('g')
            .attr('class', 'slider leftSlider')
            .call(d3.svg.axis()
              .scale(xScale)
              .tickSize(0)
              .tickFormat(''))
            .select('.domain')
            .select(function() {
              return this.parentNode.appendChild(this.cloneNode(true));
            })
          .attr('class', 'inner-bar')
          .select(function () {
            return this.parentNode.appendChild(this.cloneNode(true));
          })
        .attr('class', 'fill-bar');

        sliderBar.attr('d', 'M0,0V0H' + xScale(0) + 'V0');

        var slide = svg.append('g')
              .attr('class', 'slider sliderGroup')
              .attr('transform' , 'translate(0,10)')
              .call(brush);

        var leftHandle = slide.append('rect')
            .attr('height', 50)
            .attr('width', 7)
            .attr('x', 0)
            .attr('y', -10)
            .attr('class', 'leftHandle');

        function update(val) {
          //Update the brush position
          if(val < 0){
            val = 0;
          }
          //Testing
          if(val > sliderLimit){
            val = sliderLimit;
          }
          brush.extent([val, val]);
          //Move the slider to the correct location
          leftHandle.attr('x', val);
          console.log('Left slider is at : ' + xScale.invert(val));
          //Move the filled bar to the slider location by modifying the path
          sliderBar.attr('d', 'M0,0V0H' + val + 'V0');
        }

        //Append the Top slider
        var brush2 = d3.svg.brush()
            .x(xScale)
            .extent([0,0])
            .on('brush', slidePin);

        var svg2 = d3.select('.top-bar').append('svg')
            .attr('width', width)
            .attr('height', 20)
          .append('g');

        svg2.append('g')
            .attr('class', 'xaxisTop')
            .call(d3.svg.axis()
              .scale(xScale)
              .orient('bottom'))
          .select('.domain')
          .select(function(){ return this.parentNode.appendChild(this.cloneNode(true));})
            .attr('class', 'halo');

        var slider = svg2.append('g')
            .attr('class', 'slider')
            .attr('width', width)
            .call(brush2);

        slider.select('.background')
          .attr('height', 15);

        var pinHandle = slider.append('rect')
            .attr('width', 15)
            .attr('height', 15)
            //Container width - width of rectangle
            .attr('x', width - 15)
            .attr('y', 0)
            .attr('class', 'scrollPin');

        pinHandle.append('rect')
          .attr('width', 3)
          .attr('height', 25)
          .attr('x', 0)
          .attr('y', 0)
          .attr('class', 'scrollNeedle');

        function slidePin() {

          var pinX = d3.mouse(this)[0];
          if(pinX < 0){
            pinX = 0;
          }

          if(pinX > width-15){
            pinX = width-15;
          }

          updatePin(pinX);
        }

        function updatePin (val) {
          pinHandle.attr('x', val);
        }
      }
}

angular.module(PKG.name + '.commons')
.directive('myTimeline', function() {
  return {
    templateUrl: 'timeline/timeline.html',
    link: link
  };
});
