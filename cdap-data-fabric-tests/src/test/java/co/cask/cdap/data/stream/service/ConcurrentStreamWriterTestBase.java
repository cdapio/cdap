/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data.stream.service;

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.stream.StreamEventData;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.data.runtime.LocationStreamFileWriterFactory;
import co.cask.cdap.data.stream.InMemoryStreamCoordinatorClient;
import co.cask.cdap.data.stream.NoopStreamAdmin;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data.stream.StreamDataFileReader;
import co.cask.cdap.data.stream.StreamDataFileWriter;
import co.cask.cdap.data.stream.StreamFileTestUtils;
import co.cask.cdap.data.stream.StreamFileType;
import co.cask.cdap.data.stream.StreamFileWriterFactory;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data.stream.TimestampCloseable;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.proto.Id;
import com.google.common.base.Charsets;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for the {@link ConcurrentStreamWriter}.
 */
public abstract class ConcurrentStreamWriterTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(ConcurrentStreamWriterTestBase.class);

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static final StreamCoordinatorClient COORDINATOR_CLIENT = new InMemoryStreamCoordinatorClient();

  protected abstract LocationFactory getLocationFactory();

  @BeforeClass
  public static void startUp() {
    COORDINATOR_CLIENT.startAndWait();
  }

  @AfterClass
  public static void shutDown() {
    COORDINATOR_CLIENT.stopAndWait();
  }

  @Test
  public void testConcurrentWrite() throws Exception {
    final String streamName = "testConcurrentWrite";
    String namespace = "namespace";
    Id.Stream streamId = Id.Stream.from(namespace, streamName);
    StreamAdmin streamAdmin = new TestStreamAdmin(getLocationFactory(), Long.MAX_VALUE, 1000);
    int threads = 20;

    StreamFileWriterFactory fileWriterFactory = createStreamFileWriterFactory();
    final ConcurrentStreamWriter streamWriter = createStreamWriter(streamId, streamAdmin, threads, fileWriterFactory);

    // Starts 20 threads to write events through stream writer, each thread write 1000 events
    final int msgPerThread = 1000;
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch completion = new CountDownLatch(threads);
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    // Half of the threads write events one by one, the other half writes in batch of size 10
    for (int i = 0; i < threads / 2; i++) {
      executor.execute(createWriterTask(streamId, streamWriter,
                                        i, msgPerThread, 1, startLatch, completion));
    }
    for (int i = threads / 2; i < threads; i++) {
      executor.execute(createWriterTask(streamId, streamWriter,
                                        i, msgPerThread, 10, startLatch, completion));
    }
    startLatch.countDown();
    Assert.assertTrue(completion.await(60, TimeUnit.SECONDS));

    // Verify all events are written.
    // There should be only one partition and one file inside
    Location partitionLocation = streamAdmin.getConfig(streamId).getLocation().list().get(0);
    Location streamLocation = StreamUtils.createStreamLocation(partitionLocation,
                                                               fileWriterFactory.getFileNamePrefix(),
                                                               0, StreamFileType.EVENT);
    StreamDataFileReader reader = StreamDataFileReader.create(Locations.newInputSupplier(streamLocation));
    List<StreamEvent> events = Lists.newArrayListWithCapacity(threads * msgPerThread);
    // Should read all messages
    Assert.assertEquals(threads * msgPerThread, reader.read(events, Integer.MAX_VALUE, 0, TimeUnit.SECONDS));

    // Verify all messages as expected
    Assert.assertTrue(verifyEvents(threads, msgPerThread, events));

    reader.close();
    streamWriter.close();
  }

  @Test
  public void testConcurrentAppendFile() throws Exception {
    final String streamName = "testConcurrentFile";
    String namespace = "namespace";
    Id.Stream streamId = Id.Stream.from(namespace, streamName);
    StreamAdmin streamAdmin = new TestStreamAdmin(getLocationFactory(), Long.MAX_VALUE, 1000);
    int threads = 20;

    StreamFileWriterFactory fileWriterFactory = createStreamFileWriterFactory();
    final ConcurrentStreamWriter streamWriter = createStreamWriter(streamId, streamAdmin, threads, fileWriterFactory);

    int msgCount = 10000;
    LocationFactory locationFactory = getLocationFactory();
    // Half of the threads will be calling appendFile, then other half append event one by one

    // Prepare the files first, each file has 10000 events.
    final List<FileInfo> fileInfos = Lists.newArrayList();
    for (int i = 0; i < threads / 2; i++) {
      fileInfos.add(generateFile(locationFactory, i, msgCount));
    }

    // Append file and write events
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch completion = new CountDownLatch(threads);
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    for (int i = 0; i < threads / 2; i++) {
      executor.execute(createAppendFileTask(streamId, streamWriter,
                                            fileInfos.get(i), startLatch, completion));
    }
    for (int i = threads / 2; i < threads; i++) {
      executor.execute(createWriterTask(streamId, streamWriter, i, msgCount, 50, startLatch, completion));
    }

    startLatch.countDown();
    Assert.assertTrue(completion.await(60, TimeUnit.SECONDS));

    // Verify all events are written.
    // There should be only one partition
    Location partitionLocation = streamAdmin.getConfig(streamId).getLocation().list().get(0);
    List<Location> files = partitionLocation.list();

    List<StreamEvent> events = Lists.newArrayListWithCapacity(threads * msgCount);
    for (Location location : files) {
      // Only create reader for the event file
      if (StreamFileType.getType(location.getName()) != StreamFileType.EVENT) {
        continue;
      }
      StreamDataFileReader reader = StreamDataFileReader.create(Locations.newInputSupplier(location));
      reader.read(events, Integer.MAX_VALUE, 0, TimeUnit.SECONDS);
    }

    Assert.assertTrue(verifyEvents(threads, msgCount, events));
  }

  private boolean verifyEvents(int threads, int msgPerThread, List<StreamEvent> events) {
    Set<String> messages = Sets.newHashSet();
    for (StreamEvent event : events) {
      Assert.assertTrue(messages.add(Charsets.UTF_8.decode(event.getBody()).toString()));
    }
    for (int i = 0; i < threads; i++) {
      for (int j = 0; j < msgPerThread; j++) {
        if (!messages.contains("Message " + j + " from " + i)) {
          return false;
        }
      }
    }
    return true;
  }

  private ConcurrentStreamWriter createStreamWriter(Id.Stream streamId, StreamAdmin streamAdmin,
                                                    int threads, StreamFileWriterFactory writerFactory)
    throws Exception {
    StreamConfig streamConfig = streamAdmin.getConfig(streamId);
    streamConfig.getLocation().mkdirs();

    StreamMetaStore streamMetaStore = new InMemoryStreamMetaStore();
    streamMetaStore.addStream(streamId);
    return new ConcurrentStreamWriter(COORDINATOR_CLIENT, streamAdmin, streamMetaStore,
                                      writerFactory, threads, new TestMetricsCollectorFactory());
  }

  private Runnable createWriterTask(final Id.Stream streamId,
                                    final ConcurrentStreamWriter streamWriter,
                                    final int threadId, final int msgCount, final int batchSize,
                                    final CountDownLatch startLatch, final CountDownLatch completion) {
    return new Runnable() {
      @Override
      public void run() {
        try {
          startLatch.await();
          if (batchSize == 1) {
            // Write events one by one
            for (int j = 0; j < msgCount; j++) {
              ByteBuffer body = Charsets.UTF_8.encode("Message " + j + " from " + threadId);
              streamWriter.enqueue(streamId, ImmutableMap.<String, String>of(), body);
            }
          } else {
            // Writes event in batch of the given batch size
            final AtomicInteger written = new AtomicInteger(0);
            final MutableStreamEventData data = new MutableStreamEventData();

            while (written.get() < msgCount) {
              streamWriter.enqueue(streamId, new AbstractIterator<StreamEventData>() {
                int count = 0;

                @Override
                protected StreamEventData computeNext() {
                  // Keep returning message until returned "batchSize" messages
                  if (written.get() >= msgCount || count == batchSize) {
                    return endOfData();
                  }
                  ByteBuffer body = Charsets.UTF_8.encode("Message " + written.get() + " from " + threadId);
                  count++;
                  written.incrementAndGet();
                  return data.setBody(body);
                }
              });
            }
          }
        } catch (Exception e) {
          LOG.error("Failed to write", e);
        } finally {
          completion.countDown();
        }
      }
    };
  }

  private Runnable createAppendFileTask(final Id.Stream streamId,
                                        final ConcurrentStreamWriter streamWriter, final FileInfo fileInfo,
                                        final CountDownLatch startLatch, final CountDownLatch completion) {
    return new Runnable() {
      @Override
      public void run() {
        try {
          startLatch.await();
          streamWriter.appendFile(streamId, fileInfo.eventLocation, fileInfo.indexLocation,
                                  fileInfo.events, fileInfo.closeable);
        } catch (Exception e) {
          LOG.error("Failed to append file", e);
        } finally {
          completion.countDown();
        }
      }
    };
  }

  private StreamFileWriterFactory createStreamFileWriterFactory() {
    CConfiguration cConf = CConfiguration.create();
    return new LocationStreamFileWriterFactory(cConf);
  }

  private FileInfo generateFile(LocationFactory locationFactory, int id, int events) throws IOException {
    Location eventLocation = locationFactory.create(UUID.randomUUID().toString());
    Location indexLocation = locationFactory.create(UUID.randomUUID().toString());

    StreamDataFileWriter writer = new StreamDataFileWriter(Locations.newOutputSupplier(eventLocation),
                                                           Locations.newOutputSupplier(indexLocation),
                                                           1000L);
    for (int i = 0; i < events; i++) {
      writer.append(StreamFileTestUtils.createEvent(System.currentTimeMillis(), "Message " + i + " from " + id));
      if (i % 50 == 0) {
        writer.flush();
      }
    }
    writer.flush();
    return new FileInfo(eventLocation, indexLocation, writer, events);
  }

  private static final class FileInfo {
    private final Location eventLocation;
    private final Location indexLocation;
    private final TimestampCloseable closeable;
    private final long events;

    private FileInfo(Location eventLocation, Location indexLocation, TimestampCloseable closeable, long events) {
      this.eventLocation = eventLocation;
      this.indexLocation = indexLocation;
      this.closeable = closeable;
      this.events = events;
    }
  }

  private static final class TestStreamAdmin extends NoopStreamAdmin {

    private final LocationFactory locationFactory;
    private final long partitionDuration;
    private final long indexInterval;

    private TestStreamAdmin(LocationFactory locationFactory, long partitionDuration, long indexInterval) {
      this.locationFactory = locationFactory;
      this.partitionDuration = partitionDuration;
      this.indexInterval = indexInterval;
    }

    @Override
    public boolean exists(Id.Stream streamId) throws Exception {
      return true;
    }

    @Override
    public StreamConfig getConfig(Id.Stream streamId) throws IOException {
      Location streamLocation = StreamFileTestUtils.getStreamBaseLocation(locationFactory, streamId);
      return new StreamConfig(streamId, partitionDuration, indexInterval, Long.MAX_VALUE, streamLocation, null, 1000);
    }
  }

  private static final class TestMetricsCollectorFactory implements StreamMetricsCollectorFactory {
    @Override
    public StreamMetricsCollector createMetricsCollector(Id.Stream streamId) {
      return new StreamMetricsCollector() {
        @Override
        public void emitMetrics(long bytesWritten, long eventsWritten) {
          // No-op
        }
      };
    }
  }
}
