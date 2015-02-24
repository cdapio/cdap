/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data.stream;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.proto.Id;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Performs deletion of unused stream files.
 */
public final class StreamFileJanitor {

  private static final Logger LOG = LoggerFactory.getLogger(StreamFileJanitor.class);

  private final StreamAdmin streamAdmin;
  private final LocationFactory locationFactory;
  private final String streamBaseDirPath;

  @Inject
  public StreamFileJanitor(CConfiguration cConf, StreamAdmin streamAdmin, LocationFactory locationFactory) {
    this.streamAdmin = streamAdmin;
    this.streamBaseDirPath = cConf.get(Constants.Stream.BASE_DIR);
    this.locationFactory = locationFactory;
  }

  /**
   * Performs file cleanup for all streams.
   */
  public void cleanAll() throws IOException {
    List<Location> namespaceDirs = locationFactory.create("").list();
    for (Location namespaceDir : namespaceDirs) {
      Location streamBaseLocation = namespaceDir.append(streamBaseDirPath);
      if (!streamBaseLocation.exists()) {
        continue;
      }

      for (Location streamLocation : streamBaseLocation.list()) {
        Id.Stream streamId = StreamUtils.getStreamIdFromLocation(streamLocation);
        clean(streamAdmin.getConfig(streamId), System.currentTimeMillis());
      }
    }
  }

  /**
   * Performs deletion of unused stream file based on the given {@link StreamConfig}.
   * This method is package visible so that it can be test easily by providing a custom timestamp for current time.
   *
   * @param config Configuration of the stream to cleanup.
   * @param currentTime Current timestamp. Used for computing timestamp for expired partitions based on TTL.
   */
  @VisibleForTesting
  void clean(StreamConfig config, long currentTime) throws IOException {
    LOG.debug("Cleanup stream file for {}", config);

    // Get the current generation and remove every generations smaller then the current one.
    int generation = StreamUtils.getGeneration(config);

    for (int i = 0; i < generation; i++) {
      Location generationLocation = StreamUtils.createGenerationLocation(config.getLocation(), i);

      // Special case for generation 0
      if (generationLocation.equals(config.getLocation())) {
        for (Location location : generationLocation.list()) {
          // Only delete partition directories
          if (isPartitionDirector(location)) {
            location.delete(true);
          }
        }
      } else {
        generationLocation.delete(true);
      }
    }

    // For current generation, remove all partition directories ended older than TTL
    long expireTime = currentTime - config.getTTL();
    Location generationLocation = StreamUtils.createGenerationLocation(config.getLocation(), generation);
    for (Location location : generationLocation.list()) {
      // Only interested in partition directories
      if (!isPartitionDirector(location)) {
        continue;
      }
      long partitionEndTime = StreamUtils.getPartitionEndTime(location.getName());
      if (partitionEndTime < expireTime) {
        location.delete(true);
      }
    }
  }

  private boolean isPartitionDirector(Location location) throws IOException {
    return (location.isDirectory() && location.getName().indexOf('.') > 0);
  }
}
