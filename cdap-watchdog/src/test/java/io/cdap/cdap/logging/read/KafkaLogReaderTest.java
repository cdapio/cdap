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

package io.cdap.cdap.logging.read;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.logging.appender.LoggingTester;
import io.cdap.cdap.logging.appender.kafka.StringPartitioner;
import io.cdap.cdap.logging.context.WorkerLoggingContext;
import io.cdap.cdap.logging.filter.Filter;
import java.io.IOException;
import org.apache.twill.kafka.client.BrokerService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link KafkaLogReader} exception propagation without double-wrapping.
 */
public class KafkaLogReaderTest {

  @Test
  public void testUncheckedRuntimeExceptionNotDoubleWrappedInGetLogNextAndPrev() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Logging.KAFKA_TOPIC, "logs");
    cConf.setInt(Constants.Logging.NUM_PARTITIONS, 8);

    final IllegalStateException brokerFailure = new IllegalStateException("broker lookup failed");
    BrokerService brokerService = Mockito.mock(BrokerService.class);
    Mockito.when(brokerService.getLeader(Mockito.anyString(), Mockito.anyInt()))
        .thenThrow(brokerFailure);

    KafkaLogReader reader = new KafkaLogReader(
        cConf, new StringPartitioner(cConf), brokerService);
    LoggingContext context = new WorkerLoggingContext("default", "app1", "w1", "run1", "0");

    try {
      reader.getLogNext(context, new ReadRange(0L, 1000L, 5L), 10,
          Filter.EMPTY_FILTER, new LoggingTester.LogCallback());
      Assert.fail("Expected IllegalStateException from getLogNext");
    } catch (IllegalStateException actual) {
      Assert.assertSame(brokerFailure, actual);
    }

    try {
      reader.getLogPrev(context, ReadRange.LATEST, 10,
          Filter.EMPTY_FILTER, new LoggingTester.LogCallback());
      Assert.fail("Expected IllegalStateException from getLogPrev");
    } catch (IllegalStateException actual) {
      Assert.assertSame(brokerFailure, actual);
    }
  }

  @Test
  public void testCheckedIoExceptionWrappedInRuntimeExceptionInGetLogNextAndPrev() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Logging.KAFKA_TOPIC, "logs");
    cConf.setInt(Constants.Logging.NUM_PARTITIONS, 8);

    final IOException checkedIoFailure = new IOException("network error");
    BrokerService brokerService = Mockito.mock(BrokerService.class);
    Mockito.when(brokerService.getLeader(Mockito.anyString(), Mockito.anyInt()))
        .thenAnswer(invocation -> {
          throw checkedIoFailure;
        });

    KafkaLogReader reader = new KafkaLogReader(
        cConf, new StringPartitioner(cConf), brokerService);
    LoggingContext context = new WorkerLoggingContext("default", "app1", "w1", "run1", "0");

    try {
      reader.getLogNext(context, new ReadRange(0L, 1000L, 5L), 10,
          Filter.EMPTY_FILTER, new LoggingTester.LogCallback());
      Assert.fail("Expected RuntimeException wrapping IOException from getLogNext");
    } catch (RuntimeException actual) {
      Assert.assertEquals(RuntimeException.class, actual.getClass());
      Assert.assertSame(checkedIoFailure, actual.getCause());
    }

    try {
      reader.getLogPrev(context, ReadRange.LATEST, 10,
          Filter.EMPTY_FILTER, new LoggingTester.LogCallback());
      Assert.fail("Expected RuntimeException wrapping IOException from getLogPrev");
    } catch (RuntimeException actual) {
      Assert.assertEquals(RuntimeException.class, actual.getClass());
      Assert.assertSame(checkedIoFailure, actual.getCause());
    }
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetLogByTimeThrowsUnsupportedOperationException() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Logging.KAFKA_TOPIC, "logs");
    cConf.setInt(Constants.Logging.NUM_PARTITIONS, 8);
    KafkaLogReader reader = new KafkaLogReader(
        cConf, new StringPartitioner(cConf), Mockito.mock(BrokerService.class));
    reader.getLog(new WorkerLoggingContext("default", "app1", "w1", "r1", "0"),
        0L, 100L, Filter.EMPTY_FILTER);
  }
}
