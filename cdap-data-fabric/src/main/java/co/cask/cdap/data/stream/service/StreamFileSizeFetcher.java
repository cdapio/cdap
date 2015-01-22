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

import co.cask.cdap.data.stream.StreamFileType;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.util.List;

/**
 * Util class to get the size of files written for a stream.
 */
public class StreamFileSizeFetcher {

  /**
   * Get the size of the data persisted for the stream which config is the {@code streamConfig}.
   *
   * @param streamConfig stream to get data size of
   * @return the size of the data persisted for the stream which config is the {@code streamName}
   * @throws IOException in case of any error in fetching the size
   */
  public static long fetchSize(StreamConfig streamConfig) throws IOException {
    Location streamPath = StreamUtils.createGenerationLocation(streamConfig.getLocation(),
                                                               StreamUtils.getGeneration(streamConfig));
    long size = 0;
    List<Location> locations = streamPath.list();
    // All directories are partition directories
    for (Location location : locations) {
      if (!location.isDirectory() || !StreamUtils.isPartition(location.getName())) {
        continue;
      }

      // Note: by analyzing the size of all the files written to the FS by one stream writer,
      // we may also include data that has expired already, due to TTL. That is okay though,
      // because the point of the system is to keep track of increments of data. This initial
      // count is not meant to be accurate.

      List<Location> partitionFiles = location.list();
      for (Location partitionFile : partitionFiles) {
        if (!partitionFile.isDirectory()
          && StreamFileType.EVENT.isMatched(partitionFile.getName())) {
          size += partitionFile.length();
        }
      }
    }
    return size;
  }

  private StreamFileSizeFetcher() {
  }
}
