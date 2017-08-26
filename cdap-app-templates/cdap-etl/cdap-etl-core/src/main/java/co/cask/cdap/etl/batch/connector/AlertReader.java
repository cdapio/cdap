/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.batch.connector;

import co.cask.cdap.api.dataset.lib.AbstractCloseableIterator;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.etl.api.Alert;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import org.apache.twill.filesystem.Location;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Reads alerts written by an AlertPublisherSink.
 */
public class AlertReader extends AbstractCloseableIterator<Alert> {
  private static final Gson GSON = new Gson();
  private final Iterator<PartitionDetail> partitionIter;
  private Iterator<Location> currentLocations;
  private BufferedReader currentReader;

  public AlertReader(Collection<PartitionDetail> partitions) throws IOException {
    List<PartitionDetail> copy = new ArrayList<>(partitions);
    partitionIter = copy.iterator();
    currentReader = getNextReader();
  }

  @Override
  protected Alert computeNext() {
    if (currentReader == null) {
      return endOfData();
    }

    String line;
    try {
      while ((line = currentReader.readLine()) == null) {
        currentReader.close();
        currentReader = getNextReader();
        if (currentReader == null) {
          return endOfData();
        }
      }
      return GSON.fromJson(line, Alert.class);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void close() {
    if (currentReader != null) {
      try {
        currentReader.close();
      } catch (IOException e) {
        Throwables.propagate(e);
      }
    }
  }

  private BufferedReader getNextReader() throws IOException {
    while (currentLocations == null || !currentLocations.hasNext()) {
      if (!partitionIter.hasNext()) {
        return null;
      }
      currentLocations = partitionIter.next().getLocation().list().iterator();
      while (currentLocations.hasNext()) {
        Location file = currentLocations.next();
        String fileName = file.getName();
        // TextOutputFormat will write files like _SUCCESS and .part-m-00000.crc
        if (!"_SUCCESS".equals(fileName) && !fileName.startsWith(".")) {
          return new BufferedReader(new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8));
        }
      }
    }

    return null;
  }
}
