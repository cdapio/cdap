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
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.data2.security.ImpersonationUtils;
import co.cask.cdap.data2.security.Impersonator;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.inject.Inject;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Performs deletion of unused stream files.
 */
public final class StreamFileJanitor {

  private static final Logger LOG = LoggerFactory.getLogger(StreamFileJanitor.class);

  private final StreamAdmin streamAdmin;
  private final NamespacedLocationFactory namespacedLocationFactory;
  private final String streamBaseDirPath;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final Impersonator impersonator;

  @Inject
  public StreamFileJanitor(CConfiguration cConf, StreamAdmin streamAdmin,
                           NamespacedLocationFactory namespacedLocationFactory,
                           NamespaceQueryAdmin namespaceQueryAdmin, Impersonator impersonator) {
    this.streamAdmin = streamAdmin;
    this.streamBaseDirPath = cConf.get(Constants.Stream.BASE_DIR);
    this.namespacedLocationFactory = namespacedLocationFactory;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.impersonator = impersonator;
  }

  /**
   * Performs file cleanup for all streams.
   */
  public void cleanAll() throws Exception {
    List<NamespaceMeta> namespaces = namespaceQueryAdmin.list();
    for (final NamespaceMeta namespace : namespaces) {
      UserGroupInformation ugi = impersonator.getUGI(new NamespaceId(namespace.getName()));
      final Location streamBaseLocation = ImpersonationUtils.doAs(ugi, new Callable<Location>() {
        @Override
        public Location call() throws Exception {
          return namespacedLocationFactory.get(Id.Namespace.from(namespace.getName())).append(streamBaseDirPath);
        }
      });

      boolean exists = streamBaseLocation.exists();
      if (exists) {
        // Remove everything under the deleted directory
        Location deletedLocation = StreamUtils.getDeletedLocation(streamBaseLocation);
        if (deletedLocation.exists()) {
          Locations.deleteContent(deletedLocation);
        }
      }

      if (!exists) {
        continue;
      }

      Iterable<Location> streamLocations = StreamUtils.listAllStreams(streamBaseLocation);

      for (final Location streamLocation : streamLocations) {
        Id.Stream streamId = StreamUtils.getStreamIdFromLocation(streamLocation);
        final AtomicLong ttl = new AtomicLong(0);
        if (isStreamExists(streamId)) {
          ttl.set(streamAdmin.getConfig(streamId).getTTL());
        }
        clean(streamLocation, ttl.get(), System.currentTimeMillis());
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
