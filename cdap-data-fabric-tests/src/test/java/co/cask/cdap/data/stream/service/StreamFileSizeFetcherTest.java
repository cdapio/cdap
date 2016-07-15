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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.namespace.NamespacedLocationFactoryTestClient;
import co.cask.cdap.data.stream.NoopStreamAdmin;
import co.cask.cdap.data.stream.StreamDataFileWriter;
import co.cask.cdap.data.stream.StreamFileTestUtils;
import co.cask.cdap.data.stream.StreamFileType;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.proto.Id;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 *
 */
public class StreamFileSizeFetcherTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static final CConfiguration cConf = CConfiguration.create();
  private static NamespacedLocationFactory namespacedLocationFactory;

  @BeforeClass
  public static void init() throws IOException {
    LocationFactory locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    namespacedLocationFactory = new NamespacedLocationFactoryTestClient(cConf, locationFactory);
  }

  @Test
  public void testFetchSize() throws Exception {
    final String streamName = "testFetchSize";
    Id.Stream streamId = Id.Stream.from(Id.Namespace.DEFAULT, streamName);
    final int nbEvents = 100;
    StreamAdmin streamAdmin = new TestStreamAdmin(namespacedLocationFactory, Long.MAX_VALUE, 1000);

    streamAdmin.create(streamId);
    StreamConfig config = streamAdmin.getConfig(streamId);

    try {
      StreamUtils.fetchStreamFilesSize(StreamUtils.createGenerationLocation(config.getLocation(),
                                                                            StreamUtils.getGeneration(config)));
      Assert.fail("No stream file created yet");
    } catch (IOException e) {
      // Expected
    }

    // Creates a stream file that has no event inside
    Location partitionLocation = StreamUtils.createPartitionLocation(config.getLocation(), 0, Long.MAX_VALUE);
    Location dataLocation = StreamUtils.createStreamLocation(partitionLocation, "writer", 0, StreamFileType.EVENT);
    Location idxLocation = StreamUtils.createStreamLocation(partitionLocation, "writer", 0, StreamFileType.INDEX);
    StreamDataFileWriter writer = new StreamDataFileWriter(Locations.newOutputSupplier(dataLocation),
                                                           Locations.newOutputSupplier(idxLocation),
                                                           10000L);

    // Write 100 events to the stream
    for (int i = 0; i < nbEvents; i++) {
      writer.append(StreamFileTestUtils.createEvent(i, "foo"));
    }

    writer.close();

    long size = StreamUtils.fetchStreamFilesSize(
      StreamUtils.createGenerationLocation(config.getLocation(), StreamUtils.getGeneration(config)));
    Assert.assertTrue(size > 0);
    Assert.assertEquals(dataLocation.length(), size);
  }

  private static final class TestStreamAdmin extends NoopStreamAdmin {

    private final NamespacedLocationFactory namespacedLocationFactory;
    private final long partitionDuration;
    private final long indexInterval;

    private TestStreamAdmin(NamespacedLocationFactory namespacedLocationFactory, long partitionDuration,
                            long indexInterval) {
      this.namespacedLocationFactory = namespacedLocationFactory;
      this.partitionDuration = partitionDuration;
      this.indexInterval = indexInterval;
    }

    @Override
    public boolean exists(Id.Stream streamId) throws Exception {
      return true;
    }

    @Override
    public StreamConfig getConfig(Id.Stream streamId) throws IOException {
      Location streamLocation = StreamFileTestUtils.getStreamBaseLocation(namespacedLocationFactory, streamId);
      return new StreamConfig(streamId, partitionDuration, indexInterval, Long.MAX_VALUE, streamLocation, null, 1000);
    }
  }

}
