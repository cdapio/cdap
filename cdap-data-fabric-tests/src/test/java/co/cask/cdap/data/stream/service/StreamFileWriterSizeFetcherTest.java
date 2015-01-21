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

import co.cask.cdap.common.io.Locations;
import co.cask.cdap.data.stream.NoopStreamAdmin;
import co.cask.cdap.data.stream.StreamDataFileWriter;
import co.cask.cdap.data.stream.StreamFileTestUtils;
import co.cask.cdap.data.stream.StreamFileType;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
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
public class StreamFileWriterSizeFetcherTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static LocationFactory locationFactory;

  @BeforeClass
  public static void init() throws IOException {
    locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
  }

  @Test
  public void testFetchSize() throws Exception {
    final String streamName = "testFetchSize";
    final int nbEvents = 100;
    StreamAdmin streamAdmin = new TestStreamAdmin(locationFactory, Long.MAX_VALUE, 1000);

    streamAdmin.create(streamName);
    StreamConfig config = streamAdmin.getConfig(streamName);

    StreamWriterSizeFetcher fetcher = new StreamFileWriterSizeFetcher(0);
    try {
      fetcher.fetchSize(config);
      Assert.fail("No stream file created yet");
    } catch (IOException e) {
      // Expected
    }

    // Creates a stream file that has no event inside
    Location partitionLocation = StreamUtils.createPartitionLocation(config.getLocation(), 0, Long.MAX_VALUE);
    StreamDataFileWriter writer =
      new StreamDataFileWriter(
        Locations.newOutputSupplier(StreamUtils.createStreamLocation(partitionLocation, "writer.0", 0,
                                                                     StreamFileType.EVENT)),
        Locations.newOutputSupplier(StreamUtils.createStreamLocation(partitionLocation, "writer.0", 0,
                                                                     StreamFileType.INDEX)),
        10000L);

    // Write 100 events to the stream
    for (int i = 0; i < nbEvents; i++) {
      writer.append(StreamFileTestUtils.createEvent(i, "foo"));
    }

    writer.close();

    fetcher = new StreamFileWriterSizeFetcher(0);
    long size = fetcher.fetchSize(config);
    Assert.assertTrue(size > 0);
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
    public boolean exists(String name) throws Exception {
      return true;
    }

    @Override
    public StreamConfig getConfig(String streamName) throws IOException {
      Location streamLocation = locationFactory.create(streamName);
      return new StreamConfig(streamName, partitionDuration, indexInterval, Long.MAX_VALUE, streamLocation, null);
    }
  }

}
