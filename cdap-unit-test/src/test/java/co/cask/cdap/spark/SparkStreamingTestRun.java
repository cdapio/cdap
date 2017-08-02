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

package co.cask.cdap.spark;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.TimeseriesTable;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.kafka.KafkaTester;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.spark.app.KafkaSparkStreaming;
import co.cask.cdap.spark.app.TestSparkApp;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class SparkStreamingTestRun extends TestFrameworkTestBase {

  @ClassRule
  public static final KafkaTester KAFKA_TESTER = new KafkaTester();

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  @Test
  public void test() throws Exception {
    File checkpointDir = TEMP_FOLDER.newFolder();
    KafkaPublisher publisher = KAFKA_TESTER.getKafkaClient().getPublisher(KafkaPublisher.Ack.LEADER_RECEIVED,
                                                                          Compression.NONE);
    ApplicationManager appManager = deployApplication(TestSparkApp.class);

    Map<String, String> args = ImmutableMap.of(
      "checkpoint.path", checkpointDir.getAbsolutePath(),
      "kafka.brokers", KAFKA_TESTER.getBrokerService().getBrokerList(),
      "kafka.topics", "testtopic",
      "result.dataset", "TimeSeriesResult"
    );
    SparkManager manager = appManager.getSparkManager(KafkaSparkStreaming.class.getSimpleName());
    manager.start(args);

    // Send 100 messages over 5 seconds
    for (int i = 0; i < 100; i++) {
      publisher.prepare("testtopic").add(Charsets.UTF_8.encode("Message " + i), "1").send();
      TimeUnit.MILLISECONDS.sleep(50);
    }

    // Sum up everything from the TimeSeriesTable. The "Message" should have count 100, while each number (0-99) should
    // have count of 1
    final DataSetManager<TimeseriesTable> tsTableManager = getDataset("TimeSeriesResult");
    final TimeseriesTable tsTable = tsTableManager.get();
    Tasks.waitFor(100L, new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        tsTableManager.flush();
        return getCounts("Message", tsTable);
      }
    }, 1, TimeUnit.MINUTES, 1, TimeUnit.SECONDS);

    for (int i = 0; i < 100; i++) {
      final int finalI = i;
      Tasks.waitFor(1L, new Callable<Long>() {
        @Override
        public Long call() throws Exception {
          tsTableManager.flush();
          return getCounts(Integer.toString(finalI), tsTable);
        }
      }, 1, TimeUnit.MINUTES, 1, TimeUnit.SECONDS);
    }

    manager.stop();
    manager.waitForRun(ProgramRunStatus.KILLED, 60, TimeUnit.SECONDS);

    // Send 100 more messages without pause
    for (int i = 100; i < 200; i++) {
      publisher.prepare("testtopic").add(Charsets.UTF_8.encode("Message " + i), "1").send();
    }

    // Start the streaming app again. It should resume from where it left off because of checkpoint
    manager.start(args);

    // Expects "Message" having count = 200.
    Tasks.waitFor(100L, new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        tsTableManager.flush();
        return getCounts("Message", tsTable);
      }
    }, 1, TimeUnit.MINUTES, 1, TimeUnit.SECONDS);

    // Expects each number (0-199) have count of 1
    for (int i = 0; i < 200; i++) {
      final int finalI = i;
      Tasks.waitFor(1L, new Callable<Long>() {
        @Override
        public Long call() throws Exception {
          tsTableManager.flush();
          return getCounts(Integer.toString(finalI), tsTable);
        }
      }, 1, TimeUnit.MINUTES, 1, TimeUnit.SECONDS);
    }

    manager.stop();
    manager.waitForRuns(ProgramRunStatus.KILLED, 2, 60, TimeUnit.SECONDS);
  }

  private long getCounts(String word, TimeseriesTable tsTable) {
    long result = 0;
    Iterator<TimeseriesTable.Entry> itor = tsTable.read(Bytes.toBytes(word), 0, Long.MAX_VALUE);
    while (itor.hasNext()) {
      result += Bytes.toLong(itor.next().getValue());
    }
    return result;
  }
}
