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

package io.cdap.cdap.etl.batch.connector;

import com.google.common.base.Throwables;
import com.google.gson.Gson;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.etl.api.Alert;
import io.cdap.cdap.etl.common.Constants;
import org.apache.twill.filesystem.Location;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import javax.annotation.Nullable;

/**
 * Read alerts written by an AlertPublisherSink. Alerts will be written to files within a set of directories within a
 * base directory.
 *
 * /path/to/base/phase-1
 * /path/to/base/phase-2
 * /path/to/base/phase-3
 *
 * This reader will go through each directory in sequence and read all alerts in all files in each directory.
 */
public class AlertReader extends AbstractCloseableIterator<Alert> {
  private static final Gson GSON = new Gson();
  private final Iterator<Location> directories;
  private Iterator<Location> files;
  private BufferedReader currentReader;

  public AlertReader(FileSet fileSet) throws IOException {
    this.directories = fileSet.getBaseLocation().append(Constants.Connector.DATA_DIR).list().iterator();
    if (directories.hasNext()) {
      this.files = directories.next().list().iterator();
      this.currentReader = getNextReader();
    } else {
      this.currentReader = null;
    }
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
        throw Throwables.propagate(e);
      }
    }
  }

  @Nullable
  private BufferedReader getNextReader() throws IOException {
    while (files != null) {
      // look at the next files in the directory
      while (files.hasNext()) {
        Location file = files.next();
        String fileName = file.getName();
        // TextOutputFormat will write files like _SUCCESS and .part-m-00000.crc
        if (!"_SUCCESS".equals(fileName) && !fileName.startsWith(".")) {
          return new BufferedReader(new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8));
        }
      }

      // if we're done looking at the files in a directory and there are no more directories, we're done reading
      files = directories.hasNext() ? directories.next().list().iterator() : null;
    }

    return null;
  }

}
