/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.report.main;

import io.cdap.cdap.api.metrics.Metrics;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link RunMetaFileManager}.
 */
public class RunMetaFileManagerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testAppendRotateAndCleanupSwallowsCloseIoException() throws Exception {
    LocalLocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    Location baseLocation = locationFactory.create("run-meta");
    baseLocation.mkdirs();

    Map<String, String> runtimeArgs = new HashMap<>();
    runtimeArgs.put("file.max.size.bytes", "1");
    RunMetaFileManager manager = new RunMetaFileManager(baseLocation, runtimeArgs, createNoOpMetrics());

    ProgramRunInfo run1 = createRunInfo("ns1", "run1", 1000L);
    manager.append(run1);

    // Replace the stream in the map with one whose closeable callback throws IOException
    Map<String, RunMetaFileOutputStream> streamMap = getStreamMap(manager);
    RunMetaFileOutputStream firstStream = streamMap.get("ns1");
    Assert.assertNotNull(firstStream);
    firstStream.close();

    AtomicInteger failingCloseCount = new AtomicInteger(0);
    Location manualFile1 = baseLocation.append("ns1").append("manual-1.avro");
    manualFile1.createNew();
    RunMetaFileOutputStream failingOnRotate = new RunMetaFileOutputStream(
        manualFile1, "", 1024, 0L, () -> {
          failingCloseCount.incrementAndGet();
          throw new IOException("simulated close failure during rotation");
        });
    streamMap.put("ns1", failingOnRotate);

    // Appending triggers rotateOutputStreamIfNeeded because createTime=0L is expired;
    // Closeables.closeQuietly must swallow the IOException from close().
    ProgramRunInfo run2 = createRunInfo("ns1", "run2", 2000L);
    manager.append(run2);
    Assert.assertEquals(1, failingCloseCount.get());

    // Now replace the active stream with another failing stream and verify cleanup() swallows IOException
    RunMetaFileOutputStream secondStream = streamMap.get("ns1");
    Assert.assertNotNull(secondStream);
    secondStream.close();

    Location manualFile2 = baseLocation.append("ns1").append("manual-2.avro");
    manualFile2.createNew();
    RunMetaFileOutputStream failingOnCleanup = new RunMetaFileOutputStream(
        manualFile2, "", 1024, System.currentTimeMillis(), () -> {
          failingCloseCount.incrementAndGet();
          throw new IOException("simulated close failure during cleanup");
        });
    streamMap.put("ns1", failingOnCleanup);

    manager.cleanup();
    Assert.assertEquals(2, failingCloseCount.get());
  }

  @SuppressWarnings("unchecked")
  private Map<String, RunMetaFileOutputStream> getStreamMap(RunMetaFileManager manager)
      throws Exception {
    Field field = RunMetaFileManager.class.getDeclaredField("namespaceToLogFileStreamMap");
    field.setAccessible(true);
    return (Map<String, RunMetaFileOutputStream>) field.get(manager);
  }

  private ProgramRunInfo createRunInfo(String namespace, String runId, long timestamp) {
    ProgramRunInfo info = new ProgramRunInfo(namespace, "app1", "-SNAPSHOT", "WORKFLOW", "wf1", runId);
    info.setStatus("RUNNING");
    info.setTime(timestamp);
    info.setMessageId("msg-" + runId);
    return info;
  }

  private Metrics createNoOpMetrics() {
    return new Metrics() {
      @Override
      public void count(String metricName, int delta) {
        // no-op
      }

      @Override
      public void countLong(String metricName, long delta) {
        // no-op
      }

      @Override
      public void gauge(String metricName, long value) {
        // no-op
      }

      @Override
      public Metrics child(Map<String, String> tags) {
        return this;
      }

      @Override
      public Map<String, String> getTags() {
        return Collections.emptyMap();
      }
    };
  }
}
