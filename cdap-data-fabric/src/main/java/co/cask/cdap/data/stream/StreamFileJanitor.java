/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.store.NamespaceStore;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
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
  private final NamespacedLocationFactory namespacedLocationFactory;
  private final String streamBaseDirPath;
  private final NamespaceStore namespaceStore;

  @Inject
  public StreamFileJanitor(CConfiguration cConf, StreamAdmin streamAdmin,
                           NamespacedLocationFactory namespacedLocationFactory,
                           NamespaceStore namespaceStore) {
    this.streamAdmin = streamAdmin;
    this.streamBaseDirPath = cConf.get(Constants.Stream.BASE_DIR);
    this.namespacedLocationFactory = namespacedLocationFactory;
    this.namespaceStore = namespaceStore;
  }

  /**
   * Performs file cleanup for all streams.
   */
  public void cleanAll() throws Exception {
    List<NamespaceMeta> namespaces = namespaceStore.list();

    for (NamespaceMeta namespace : namespaces) {
      Location streamBaseLocation =
        namespacedLocationFactory.get(Id.Namespace.from(namespace.getName())).append(streamBaseDirPath);
      if (!streamBaseLocation.exists()) {
        continue;
      }

      // Remove everything under the deleted directory
      Location deletedLocation = StreamUtils.getDeletedLocation(streamBaseLocation);
      if (deletedLocation.exists()) {
        Locations.deleteContent(deletedLocation);
      }

      for (Location streamLocation : StreamUtils.listAllStreams(streamBaseLocation)) {
        Id.Stream streamId = StreamUtils.getStreamIdFromLocation(streamLocation);
        long ttl = 0L;
        if (isStreamExists(streamId)) {
          ttl = streamAdmin.getConfig(streamId).getTTL();
        }
        clean(streamLocation, ttl, System.currentTimeMillis());
      }
    }
  }

  /**
   * Performs deletion of unused stream file based on the given {@link StreamConfig}.
   * This method is package visible so that it can be test easily by providing a custom timestamp for current time.
   *
   * @param streamLocation stream location
   * @param ttl ttl for the cleanup
   * @param currentTime Current timestamp. Used for computing timestamp for expired partitions based on TTL.
   */
  @VisibleForTesting
  void clean(Location streamLocation, long ttl, long currentTime) throws IOException {
    LOG.debug("Cleanup stream file in {}", streamLocation);

    // Get the current generation and remove every generations smaller then the current one.
    int generation = StreamUtils.getGeneration(streamLocation);

    for (int i = 0; i < generation; i++) {
      Location generationLocation = StreamUtils.createGenerationLocation(streamLocation, i);

      // Special case for generation 0
      if (generationLocation.equals(streamLocation)) {
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
    long expireTime = currentTime - ttl;
    Location generationLocation = StreamUtils.createGenerationLocation(streamLocation, generation);
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

  private boolean isStreamExists(Id.Stream streamId) throws IOException {
    try {
      return streamAdmin.exists(streamId);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
