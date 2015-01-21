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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.stream.StreamFileType;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.util.List;

/**
 * Implementation of the {@link StreamWriterSizeFetcher} for Streams written on files.
 */
public class StreamFileWriterSizeFetcher implements StreamWriterSizeFetcher {

  private final int instanceId;

  @Inject
  public StreamFileWriterSizeFetcher(@Named(Constants.Stream.CONTAINER_INSTANCE_ID) int instanceId) {
    this.instanceId = instanceId;
  }

  @Override
  public long fetchSize(StreamConfig streamConfig) throws IOException {
    Location streamPath = StreamUtils.createGenerationLocation(streamConfig.getLocation(),
                                                               StreamUtils.getGeneration(streamConfig));
    long size = 0;
    List<Location> locations = streamPath.list();
    // All directories are partition directories
    for (Location location : locations) {
      if (!location.isDirectory()) {
        continue;
      }

      // Note: by analyzing the size of all the files written to the FS by one stream writer,
      // we may also include data that has expired already, due to TTL. That is okay though,
      // because the point of the system is to keep track of increments of data. This initial
      // count is not meant to be accurate.

      List<Location> partitionFiles = location.list();
      for (Location partitionFile : partitionFiles) {
        if (!partitionFile.isDirectory()
          && StreamFileType.EVENT.isMatched(partitionFile.getName())
          && StreamUtils.getWriterInstanceId(partitionFile.getName()) == instanceId) {
          size += partitionFile.length();
        }
      }
    }
    return size;
  }
}
