package com.continuuity.data.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.file.FileWriter;
import com.continuuity.data2.transaction.stream.AbstractStreamFileAdmin;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.continuuity.data2.transaction.stream.StreamConsumerStateStoreFactory;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * Base test class for stream file janitor.
 */
public abstract class StreamFileJanitorTestBase {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  protected abstract LocationFactory getLocationFactory();

  protected abstract StreamAdmin getStreamAdmin();

  protected abstract CConfiguration getCConfiguration();

  protected abstract FileWriter<StreamEvent> createWriter(String streamName) throws IOException;

  @Test
  public void testCleanupGeneration() throws Exception {
    // Create a stream and performs couple truncate
    String streamName = "testCleanupGeneration";
    StreamAdmin streamAdmin = getStreamAdmin();
    streamAdmin.create(streamName);
    StreamConfig streamConfig = streamAdmin.getConfig(streamName);
    StreamFileJanitor janitor = new StreamFileJanitor(getCConfiguration(), getStreamAdmin(), getLocationFactory());

    for (int i = 0; i < 5; i++) {
      FileWriter<StreamEvent> writer = createWriter(streamName);
      writer.append(StreamFileTestUtils.createEvent(System.currentTimeMillis(), "Testing"));
      writer.close();

      // Call cleanup before truncate. The current generation should stand.
      janitor.clean(streamConfig, System.currentTimeMillis());
      verifyGeneration(streamConfig, i);

      streamAdmin.truncate(streamName);
    }

    int generation = StreamUtils.getGeneration(streamConfig);
    Assert.assertEquals(5, generation);

    janitor.clean(streamConfig, System.currentTimeMillis());

    // Verify the stream directory should only contains the generation directory
    for (Location location : streamConfig.getLocation().list()) {
      if (location.isDirectory()) {
        Assert.assertEquals(generation, Integer.parseInt(location.getName()));
      }
    }
  }

  @Test
  public void testCleanupTTL() throws Exception {
    // Create a stream with 5 seconds TTL, partition duration of 2 seconds
    String streamName = "testCleanupTTL";
    StreamAdmin streamAdmin = getStreamAdmin();
    StreamFileJanitor janitor = new StreamFileJanitor(getCConfiguration(), getStreamAdmin(), getLocationFactory());

    Properties properties = new Properties();
    properties.setProperty(Constants.Stream.PARTITION_DURATION, "2000");
    properties.setProperty(Constants.Stream.TTL, "5000");

    streamAdmin.create(streamName, properties);

    // Truncate to increment generation to 1. This make verification condition easier (won't affect correctness).
    streamAdmin.truncate(streamName);
    StreamConfig config = streamAdmin.getConfig(streamName);

    // Write data with different timestamps that spans across 5 partitions
    FileWriter<StreamEvent> writer = createWriter(streamName);

    for (int i = 0; i < 10; i++) {
      writer.append(StreamFileTestUtils.createEvent(i * 1000, "Testing " + i));
    }
    writer.close();

    // Should see 5 partitions
    Location generationLocation = StreamUtils.createGenerationLocation(config.getLocation(), 1);
    Assert.assertEquals(5, generationLocation.list().size());

    // Perform clean with current time = 10000 (10 seconds since epoch).
    // Since TTL = 5 seconds, 2 partitions will be remove (Ends at 2000 and ends at 4000).
    janitor.clean(config, 10000);

    Assert.assertEquals(3, generationLocation.list().size());

    // Cleanup again with current time = 16000, all partitions should be deleted.
    janitor.clean(config, 16000);
    Assert.assertTrue(generationLocation.list().isEmpty());
  }

  private void verifyGeneration(StreamConfig config, int generation) throws IOException {
    Location generationLocation = StreamUtils.createGenerationLocation(config.getLocation(), generation);
    Assert.assertTrue(generationLocation.isDirectory());

    // There should be a partition directory inside
    for (Location location : generationLocation.list()) {
      if (location.isDirectory() && location.getName().indexOf('.') > 0) {
        return;
      }
    }

    throw new IOException("Not a valid generation directory");
  }

  /**
   * A stream admin for interact with files only (the product one operations on both file and HBase).
   */
  protected static final class TestStreamFileAdmin extends AbstractStreamFileAdmin {

    @Inject
    TestStreamFileAdmin(LocationFactory locationFactory, CConfiguration cConf, StreamCoordinator
                        streamCoordinator, StreamConsumerStateStoreFactory stateStoreFactory) {
      super(locationFactory, cConf, streamCoordinator, stateStoreFactory, new NoopStreamAdmin());
    }
  }

  /**
   * A {@link StreamAdmin} that does nothing.
   */
  protected static final class NoopStreamAdmin implements StreamAdmin {

    @Override
    public void dropAll() throws Exception {
    }

    @Override
    public void configureInstances(QueueName streamName, long groupId, int instances) throws Exception {
    }

    @Override
    public void configureGroups(QueueName streamName, Map<Long, Integer> groupInfo) throws Exception {
    }

    @Override
    public void upgrade() throws Exception {
    }

    @Override
    public StreamConfig getConfig(String streamName) throws IOException {
      throw new IllegalStateException("Stream " + streamName + " not exists.");
    }

    @Override
    public void updateConfig(StreamConfig config) throws IOException {
    }

    @Override
    public boolean exists(String name) throws Exception {
      return false;
    }

    @Override
    public void create(String name) throws Exception {
    }

    @Override
    public void create(String name, @Nullable Properties props) throws Exception {
    }

    @Override
    public void truncate(String name) throws Exception {
    }

    @Override
    public void drop(String name) throws Exception {
    }

    @Override
    public void upgrade(String name, Properties properties) throws Exception {
    }
  }
}
