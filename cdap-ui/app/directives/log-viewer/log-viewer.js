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

function LogViewerController ($scope) {
  'ngInject';

  this.configOptions = {
    time: true,
    level: true,
    source: true,
    message: true
  };

  angular.forEach($scope.displayOptions, (value, key) => {
    this.configOptions[key] = value;
  });

  this.logEvents = ['ERROR', 'WARN', 'INFO', 'DEBUG', 'TRACE'];

  let included = {
    'ERROR' : false,
    'WARN' : false,
    'INFO' : false,
    'DEBUG' : false,
    'TRACE' : false
  };

  this.data = [
    {
      time: '2016-03-04 16:28:40, 798',
      level: 'INFO',
      source: 'leader-election-election-metrics-processor-part-0',
      message: {
        content: 'Got exception publishing audit message AuditMessage{version=1, time=1465850244499, entityId=dataset:default._kafkaOffset, user=\'\', type=ACCESS, payload=AccessPayload{accessType=UNKNOWN, accessor=program_run:default._Tracker.flow.AuditLogFlow.a1e96ae1-31a6-11e6-8c41-e285682178e6} AuditPayload{}}.',
        stackTrace: 'java.util.concurrent.ExecutionException: java.lang.IllegalStateException: No kafka producer available.\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.getValue(AbstractFuture.java:294) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.get(AbstractFuture.java:281) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture.get(AbstractFuture.java:116) ~[guava-13.0.1.jar:na]\n\tat co.cask.cdap.data2.audit.KafkaAuditPublisher.publish(KafkaAuditPublisher.java:72) ~[classes/:na]\n\tat co.cask.cdap.data2.audit.AuditPublishers.publishAccess(AuditPublishers.java:90) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.doWriteLineage(LineageWriterDatasetFramework.java:187) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.writeLineage(LineageWriterDatasetFramework.java:169) [classes/:na]\n\tat co.cask.cdap.data.dataset.SystemDatasetInstantiator.writeLineage(SystemDatasetInstantiator.java:108) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:143) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:127) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache.getDataset(SingleThreadDatasetCache.java:170) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:150) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:126) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:179) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:174) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:168) [classes/:na]\n\tat co.cask.tracker.AuditLogConsumer.configureKafka(AuditLogConsumer.java:92) [unpacked/:na]\n\tat co.cask.cdap.kafka.flow.KafkaConsumerFlowlet.initialize(KafkaConsumerFlowlet.java:101) [cdap-kafka-flow-core-0.9.0.jar:na]\n\tat co.cask.cdap.kafka.flow.Kafka08ConsumerFlowlet.initialize(Kafka08ConsumerFlowlet.java:97) [cdap-kafka-flow-compat-0.8-0.9.0.jar:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService$1.apply(FlowletRuntimeService.java:115) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:92) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeOnce(DynamicTransactionExecutor.java:125) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeWithRetry(DynamicTransactionExecutor.java:104) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:61) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.initFlowlet(FlowletRuntimeService.java:109) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.startUp(FlowletRuntimeService.java:71) [classes/:na]\n\tat com.google.common.util.concurrent.AbstractIdleService$1$1.run(AbstractIdleService.java:43) [guava-13.0.1.jar:na]\n\tat java.lang.Thread.run(Thread.java:745) [na:1.7.0_80]'
      }
    },
    {
      time: '2016-03-04 16:28:43, 801',
      level: 'ERROR',
      source: 'leader-election-election-metrics-processor-part-0',
      message: {
        content: 'Some Other log data that is irrelevant for this demo.'
      }
    },
    {
      time: '2016-03-04 16:28:43, 900',
      level: 'WARN',
      source: 'leader-election-election-metrics-processor-part-0',
      message: {
        content: 'Got exception publishing audit message AuditMessage{version=1, time=1465850244499, entityId=dataset:default._kafkaOffset, user=\'\', type=ACCESS, payload=AccessPayload{accessType=UNKNOWN, accessor=program_run:default._Tracker.flow.AuditLogFlow.a1e96ae1-31a6-11e6-8c41-e285682178e6} AuditPayload{}}.',
        stackTrace: 'java.util.concurrent.ExecutionException: java.lang.IllegalStateException: No kafka producer available.\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.getValue(AbstractFuture.java:294) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.get(AbstractFuture.java:281) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture.get(AbstractFuture.java:116) ~[guava-13.0.1.jar:na]\n\tat co.cask.cdap.data2.audit.KafkaAuditPublisher.publish(KafkaAuditPublisher.java:72) ~[classes/:na]\n\tat co.cask.cdap.data2.audit.AuditPublishers.publishAccess(AuditPublishers.java:90) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.doWriteLineage(LineageWriterDatasetFramework.java:187) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.writeLineage(LineageWriterDatasetFramework.java:169) [classes/:na]\n\tat co.cask.cdap.data.dataset.SystemDatasetInstantiator.writeLineage(SystemDatasetInstantiator.java:108) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:143) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:127) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache.getDataset(SingleThreadDatasetCache.java:170) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:150) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:126) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:179) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:174) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:168) [classes/:na]\n\tat co.cask.tracker.AuditLogConsumer.configureKafka(AuditLogConsumer.java:92) [unpacked/:na]\n\tat co.cask.cdap.kafka.flow.KafkaConsumerFlowlet.initialize(KafkaConsumerFlowlet.java:101) [cdap-kafka-flow-core-0.9.0.jar:na]\n\tat co.cask.cdap.kafka.flow.Kafka08ConsumerFlowlet.initialize(Kafka08ConsumerFlowlet.java:97) [cdap-kafka-flow-compat-0.8-0.9.0.jar:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService$1.apply(FlowletRuntimeService.java:115) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:92) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeOnce(DynamicTransactionExecutor.java:125) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeWithRetry(DynamicTransactionExecutor.java:104) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:61) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.initFlowlet(FlowletRuntimeService.java:109) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.startUp(FlowletRuntimeService.java:71) [classes/:na]\n\tat com.google.common.util.concurrent.AbstractIdleService$1$1.run(AbstractIdleService.java:43) [guava-13.0.1.jar:na]\n\tat java.lang.Thread.run(Thread.java:745) [na:1.7.0_80]'
      }
    },
    {
      time: '2016-03-04 16:28:40, 798',
      level: 'DEBUG',
      source: 'leader-election-election-metrics-processor-part-0',
      message: {
        content: 'Got exception publishing audit message AuditMessage{version=1, time=1465850244499, entityId=dataset:default._kafkaOffset, user=\'\', type=ACCESS, payload=AccessPayload{accessType=UNKNOWN, accessor=program_run:default._Tracker.flow.AuditLogFlow.a1e96ae1-31a6-11e6-8c41-e285682178e6} AuditPayload{}}.',
        stackTrace: 'java.util.concurrent.ExecutionException: java.lang.IllegalStateException: No kafka producer available.\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.getValue(AbstractFuture.java:294) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.get(AbstractFuture.java:281) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture.get(AbstractFuture.java:116) ~[guava-13.0.1.jar:na]\n\tat co.cask.cdap.data2.audit.KafkaAuditPublisher.publish(KafkaAuditPublisher.java:72) ~[classes/:na]\n\tat co.cask.cdap.data2.audit.AuditPublishers.publishAccess(AuditPublishers.java:90) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.doWriteLineage(LineageWriterDatasetFramework.java:187) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.writeLineage(LineageWriterDatasetFramework.java:169) [classes/:na]\n\tat co.cask.cdap.data.dataset.SystemDatasetInstantiator.writeLineage(SystemDatasetInstantiator.java:108) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:143) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:127) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache.getDataset(SingleThreadDatasetCache.java:170) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:150) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:126) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:179) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:174) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:168) [classes/:na]\n\tat co.cask.tracker.AuditLogConsumer.configureKafka(AuditLogConsumer.java:92) [unpacked/:na]\n\tat co.cask.cdap.kafka.flow.KafkaConsumerFlowlet.initialize(KafkaConsumerFlowlet.java:101) [cdap-kafka-flow-core-0.9.0.jar:na]\n\tat co.cask.cdap.kafka.flow.Kafka08ConsumerFlowlet.initialize(Kafka08ConsumerFlowlet.java:97) [cdap-kafka-flow-compat-0.8-0.9.0.jar:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService$1.apply(FlowletRuntimeService.java:115) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:92) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeOnce(DynamicTransactionExecutor.java:125) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeWithRetry(DynamicTransactionExecutor.java:104) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:61) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.initFlowlet(FlowletRuntimeService.java:109) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.startUp(FlowletRuntimeService.java:71) [classes/:na]\n\tat com.google.common.util.concurrent.AbstractIdleService$1$1.run(AbstractIdleService.java:43) [guava-13.0.1.jar:na]\n\tat java.lang.Thread.run(Thread.java:745) [na:1.7.0_80]'
      }
    },
    {
      time: '2016-03-04 16:28:43, 801',
      level: 'TRACE',
      source: 'leader-election-election-metrics-processor-part-0',
      message: {
        content: 'Some Other log data that is irrelevant for this demo.'
      }
    },
    {
      time: '2016-03-04 16:28:43, 900',
      level: 'INFO',
      source: 'leader-election-election-metrics-processor-part-0',
      message: {
        content: 'Got exception publishing audit message AuditMessage{version=1, time=1465850244499, entityId=dataset:default._kafkaOffset, user=\'\', type=ACCESS, payload=AccessPayload{accessType=UNKNOWN, accessor=program_run:default._Tracker.flow.AuditLogFlow.a1e96ae1-31a6-11e6-8c41-e285682178e6} AuditPayload{}}.',
        stackTrace: 'java.util.concurrent.ExecutionException: java.lang.IllegalStateException: No kafka producer available.\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.getValue(AbstractFuture.java:294) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.get(AbstractFuture.java:281) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture.get(AbstractFuture.java:116) ~[guava-13.0.1.jar:na]\n\tat co.cask.cdap.data2.audit.KafkaAuditPublisher.publish(KafkaAuditPublisher.java:72) ~[classes/:na]\n\tat co.cask.cdap.data2.audit.AuditPublishers.publishAccess(AuditPublishers.java:90) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.doWriteLineage(LineageWriterDatasetFramework.java:187) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.writeLineage(LineageWriterDatasetFramework.java:169) [classes/:na]\n\tat co.cask.cdap.data.dataset.SystemDatasetInstantiator.writeLineage(SystemDatasetInstantiator.java:108) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:143) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:127) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache.getDataset(SingleThreadDatasetCache.java:170) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:150) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:126) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:179) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:174) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:168) [classes/:na]\n\tat co.cask.tracker.AuditLogConsumer.configureKafka(AuditLogConsumer.java:92) [unpacked/:na]\n\tat co.cask.cdap.kafka.flow.KafkaConsumerFlowlet.initialize(KafkaConsumerFlowlet.java:101) [cdap-kafka-flow-core-0.9.0.jar:na]\n\tat co.cask.cdap.kafka.flow.Kafka08ConsumerFlowlet.initialize(Kafka08ConsumerFlowlet.java:97) [cdap-kafka-flow-compat-0.8-0.9.0.jar:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService$1.apply(FlowletRuntimeService.java:115) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:92) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeOnce(DynamicTransactionExecutor.java:125) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeWithRetry(DynamicTransactionExecutor.java:104) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:61) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.initFlowlet(FlowletRuntimeService.java:109) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.startUp(FlowletRuntimeService.java:71) [classes/:na]\n\tat com.google.common.util.concurrent.AbstractIdleService$1$1.run(AbstractIdleService.java:43) [guava-13.0.1.jar:na]\n\tat java.lang.Thread.run(Thread.java:745) [na:1.7.0_80]'
      }
    },
    {
      time: '2016-03-04 16:28:43, 801',
      level: 'ERROR',
      source: 'leader-election-election-metrics-processor-part-0',
      message: {
        content: 'Some Other log data that is irrelevant for this demo.'
      }
    },
    {
      time: '2016-03-04 16:28:43, 900',
      level: 'WARN',
      source: 'leader-election-election-metrics-processor-part-0',
      message: {
        content: 'Got exception publishing audit message AuditMessage{version=1, time=1465850244499, entityId=dataset:default._kafkaOffset, user=\'\', type=ACCESS, payload=AccessPayload{accessType=UNKNOWN, accessor=program_run:default._Tracker.flow.AuditLogFlow.a1e96ae1-31a6-11e6-8c41-e285682178e6} AuditPayload{}}.',
        stackTrace: 'java.util.concurrent.ExecutionException: java.lang.IllegalStateException: No kafka producer available.\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.getValue(AbstractFuture.java:294) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.get(AbstractFuture.java:281) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture.get(AbstractFuture.java:116) ~[guava-13.0.1.jar:na]\n\tat co.cask.cdap.data2.audit.KafkaAuditPublisher.publish(KafkaAuditPublisher.java:72) ~[classes/:na]\n\tat co.cask.cdap.data2.audit.AuditPublishers.publishAccess(AuditPublishers.java:90) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.doWriteLineage(LineageWriterDatasetFramework.java:187) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.writeLineage(LineageWriterDatasetFramework.java:169) [classes/:na]\n\tat co.cask.cdap.data.dataset.SystemDatasetInstantiator.writeLineage(SystemDatasetInstantiator.java:108) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:143) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:127) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache.getDataset(SingleThreadDatasetCache.java:170) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:150) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:126) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:179) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:174) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:168) [classes/:na]\n\tat co.cask.tracker.AuditLogConsumer.configureKafka(AuditLogConsumer.java:92) [unpacked/:na]\n\tat co.cask.cdap.kafka.flow.KafkaConsumerFlowlet.initialize(KafkaConsumerFlowlet.java:101) [cdap-kafka-flow-core-0.9.0.jar:na]\n\tat co.cask.cdap.kafka.flow.Kafka08ConsumerFlowlet.initialize(Kafka08ConsumerFlowlet.java:97) [cdap-kafka-flow-compat-0.8-0.9.0.jar:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService$1.apply(FlowletRuntimeService.java:115) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:92) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeOnce(DynamicTransactionExecutor.java:125) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeWithRetry(DynamicTransactionExecutor.java:104) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:61) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.initFlowlet(FlowletRuntimeService.java:109) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.startUp(FlowletRuntimeService.java:71) [classes/:na]\n\tat com.google.common.util.concurrent.AbstractIdleService$1$1.run(AbstractIdleService.java:43) [guava-13.0.1.jar:na]\n\tat java.lang.Thread.run(Thread.java:745) [na:1.7.0_80]'
      }
    },
    {
      time: '2016-03-04 16:28:40, 798',
      level: 'DEBUG',
      source: 'leader-election-election-metrics-processor-part-0',
      message: {
        content: 'Got exception publishing audit message AuditMessage{version=1, time=1465850244499, entityId=dataset:default._kafkaOffset, user=\'\', type=ACCESS, payload=AccessPayload{accessType=UNKNOWN, accessor=program_run:default._Tracker.flow.AuditLogFlow.a1e96ae1-31a6-11e6-8c41-e285682178e6} AuditPayload{}}.',
        stackTrace: 'java.util.concurrent.ExecutionException: java.lang.IllegalStateException: No kafka producer available.\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.getValue(AbstractFuture.java:294) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.get(AbstractFuture.java:281) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture.get(AbstractFuture.java:116) ~[guava-13.0.1.jar:na]\n\tat co.cask.cdap.data2.audit.KafkaAuditPublisher.publish(KafkaAuditPublisher.java:72) ~[classes/:na]\n\tat co.cask.cdap.data2.audit.AuditPublishers.publishAccess(AuditPublishers.java:90) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.doWriteLineage(LineageWriterDatasetFramework.java:187) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.writeLineage(LineageWriterDatasetFramework.java:169) [classes/:na]\n\tat co.cask.cdap.data.dataset.SystemDatasetInstantiator.writeLineage(SystemDatasetInstantiator.java:108) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:143) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:127) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache.getDataset(SingleThreadDatasetCache.java:170) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:150) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:126) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:179) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:174) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:168) [classes/:na]\n\tat co.cask.tracker.AuditLogConsumer.configureKafka(AuditLogConsumer.java:92) [unpacked/:na]\n\tat co.cask.cdap.kafka.flow.KafkaConsumerFlowlet.initialize(KafkaConsumerFlowlet.java:101) [cdap-kafka-flow-core-0.9.0.jar:na]\n\tat co.cask.cdap.kafka.flow.Kafka08ConsumerFlowlet.initialize(Kafka08ConsumerFlowlet.java:97) [cdap-kafka-flow-compat-0.8-0.9.0.jar:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService$1.apply(FlowletRuntimeService.java:115) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:92) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeOnce(DynamicTransactionExecutor.java:125) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeWithRetry(DynamicTransactionExecutor.java:104) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:61) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.initFlowlet(FlowletRuntimeService.java:109) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.startUp(FlowletRuntimeService.java:71) [classes/:na]\n\tat com.google.common.util.concurrent.AbstractIdleService$1$1.run(AbstractIdleService.java:43) [guava-13.0.1.jar:na]\n\tat java.lang.Thread.run(Thread.java:745) [na:1.7.0_80]'
      }
    },
    {
      time: '2016-03-04 16:28:43, 801',
      level: 'TRACE',
      source: 'leader-election-election-metrics-processor-part-0',
      message: {
        content: 'Some Other log data that is irrelevant for this demo.'
      }
    },
    {
      time: '2016-03-04 16:28:43, 900',
      level: 'INFO',
      source: 'leader-election-election-metrics-processor-part-0',
      message: {
        content: 'Got exception publishing audit message AuditMessage{version=1, time=1465850244499, entityId=dataset:default._kafkaOffset, user=\'\', type=ACCESS, payload=AccessPayload{accessType=UNKNOWN, accessor=program_run:default._Tracker.flow.AuditLogFlow.a1e96ae1-31a6-11e6-8c41-e285682178e6} AuditPayload{}}.',
        stackTrace: 'java.util.concurrent.ExecutionException: java.lang.IllegalStateException: No kafka producer available.\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.getValue(AbstractFuture.java:294) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture$Sync.get(AbstractFuture.java:281) ~[guava-13.0.1.jar:na]\n\tat com.google.common.util.concurrent.AbstractFuture.get(AbstractFuture.java:116) ~[guava-13.0.1.jar:na]\n\tat co.cask.cdap.data2.audit.KafkaAuditPublisher.publish(KafkaAuditPublisher.java:72) ~[classes/:na]\n\tat co.cask.cdap.data2.audit.AuditPublishers.publishAccess(AuditPublishers.java:90) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.doWriteLineage(LineageWriterDatasetFramework.java:187) [classes/:na]\n\tat co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework.writeLineage(LineageWriterDatasetFramework.java:169) [classes/:na]\n\tat co.cask.cdap.data.dataset.SystemDatasetInstantiator.writeLineage(SystemDatasetInstantiator.java:108) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:143) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache$LineageRecordingDatasetCache.get(SingleThreadDatasetCache.java:127) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.SingleThreadDatasetCache.getDataset(SingleThreadDatasetCache.java:170) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:150) [classes/:na]\n\tat co.cask.cdap.data2.dataset2.DynamicDatasetCache.getDataset(DynamicDatasetCache.java:126) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:179) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:174) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.AbstractContext.getDataset(AbstractContext.java:168) [classes/:na]\n\tat co.cask.tracker.AuditLogConsumer.configureKafka(AuditLogConsumer.java:92) [unpacked/:na]\n\tat co.cask.cdap.kafka.flow.KafkaConsumerFlowlet.initialize(KafkaConsumerFlowlet.java:101) [cdap-kafka-flow-core-0.9.0.jar:na]\n\tat co.cask.cdap.kafka.flow.Kafka08ConsumerFlowlet.initialize(Kafka08ConsumerFlowlet.java:97) [cdap-kafka-flow-compat-0.8-0.9.0.jar:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService$1.apply(FlowletRuntimeService.java:115) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:92) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor$3.apply(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeOnce(DynamicTransactionExecutor.java:125) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.executeWithRetry(DynamicTransactionExecutor.java:104) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:61) [classes/:na]\n\tat co.cask.cdap.data2.transaction.DynamicTransactionExecutor.execute(DynamicTransactionExecutor.java:89) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.initFlowlet(FlowletRuntimeService.java:109) [classes/:na]\n\tat co.cask.cdap.internal.app.runtime.flow.FlowletRuntimeService.startUp(FlowletRuntimeService.java:71) [classes/:na]\n\tat com.google.common.util.concurrent.AbstractIdleService$1$1.run(AbstractIdleService.java:43) [guava-13.0.1.jar:na]\n\tat java.lang.Thread.run(Thread.java:745) [na:1.7.0_80]'
      }
    },
  ];

  this.totalCount = this.data.length;
  let errorCount = 0;
  let warningCount = 0;
  let numEvents = 0;

  //Compute Total
  for(let k = 0; k < this.data.length; k++){
    let currentItem = this.data[k].level;
    if(currentItem === 'ERROR'){
      errorCount++;
    } else if(currentItem === 'WARN'){
      warningCount++;
    }
  }

  this.errorCount = errorCount;
  this.warningCount = warningCount;

  //New 'includeEvent' function
  this.includeEvent = function(eventType){
    if(included[eventType]){
      numEvents--;
    } else{
      numEvents++;
    }
    included[eventType] = !included[eventType];
  };

  this.eventFilter = function(log){
    if(numEvents === 0 || included[log.level]){
      return log;
    }
    return;
  };
}

angular.module(PKG.name + '.commons')
  .directive('myLogViewer', function () {
    return {
      templateUrl: 'log-viewer/log-viewer.html',
      controller: LogViewerController,
      scope: {
        displayOptions: '=?'
      },
      controllerAs: 'LogViewer'
    };
  });
