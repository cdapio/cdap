/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.logging.pipeline.kafka;

import ch.qos.logback.classic.LoggerContext;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.metrics.NoopMetricsContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.logging.meta.Checkpoint;
import io.cdap.cdap.logging.meta.CheckpointManager;
import io.cdap.cdap.logging.meta.KafkaOffset;
import io.cdap.cdap.logging.pipeline.LogPipelineTestUtil;
import io.cdap.cdap.logging.pipeline.LogProcessorPipelineContext;
import io.cdap.cdap.logging.pipeline.MockAppender;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.twill.kafka.client.BrokerService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Fast unit tests for {@link KafkaLogProcessorPipeline} service naming and start/stop lifecycle.
 */
public class KafkaLogProcessorPipelineServiceTest {

  @Test
  public void testServiceNameThreadNameAndLifecycle() throws Exception {
    LoggerContext loggerContext = LogPipelineTestUtil.createLoggerContext(
        "WARN", ImmutableMap.of("test.logger", "INFO"), MockAppender.class.getName());
    LogProcessorPipelineContext pipelineContext = new LogProcessorPipelineContext(
        CConfiguration.create(), "auditKafka", loggerContext, new NoopMetricsContext(), 0);

    final AtomicReference<String> startupThreadName = new AtomicReference<>();
    final AtomicReference<Map<Integer, ? extends Checkpoint<KafkaOffset>>> savedOnStop =
        new AtomicReference<>();
    CheckpointManager<KafkaOffset> checkpointManager = new CheckpointManager<KafkaOffset>() {
      @Override
      public void saveCheckpoints(Map<Integer, ? extends Checkpoint<KafkaOffset>> checkpoints) {
        savedOnStop.set(checkpoints);
      }

      @Override
      public Map<Integer, Checkpoint<KafkaOffset>> getCheckpoint(Set<Integer> partitions) {
        startupThreadName.set(Thread.currentThread().getName());
        return Collections.emptyMap();
      }

      @Override
      public Checkpoint<KafkaOffset> getCheckpoint(int partition) {
        return new Checkpoint<>(new KafkaOffset(-1, -1), -1);
      }
    };

    KafkaPipelineConfig config = new KafkaPipelineConfig(
        "logs", Collections.singleton(0), 1024L, 100L, 65536, 500L);
    KafkaLogProcessorPipeline pipeline = new KafkaLogProcessorPipeline(
        pipelineContext, checkpointManager, Mockito.mock(BrokerService.class), config);

    Assert.assertEquals("LogPipeline-auditKafka [NEW]", pipeline.toString());
    pipeline.startAndWait();
    Assert.assertEquals("LogPipeline-auditKafka [RUNNING]", pipeline.toString());
    Assert.assertEquals("LogPipeline-auditKafka", startupThreadName.get());

    pipeline.stopAndWait();
    Assert.assertEquals("LogPipeline-auditKafka [TERMINATED]", pipeline.toString());
    Assert.assertNotNull(savedOnStop.get());
  }
}
