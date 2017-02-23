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

package co.cask.cdap.logging.plugins;

import co.cask.cdap.common.io.Syncable;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Location outputstream used by {@link LocationManager} which holds location, number of bytes written to a location
 * and its open outputstream
 */
public class LocationOutputStream extends FilterOutputStream implements Syncable {
  private static final Logger LOG = LoggerFactory.getLogger(LocationOutputStream.class);

  private Location location;
  private long numOfBytes;
  private long lastWriteTimestamp;

  public LocationOutputStream(Location location, OutputStream outputStream, long lastWriteTimestamp) {
    super(outputStream);
    this.location = location;
    this.lastWriteTimestamp = lastWriteTimestamp;
  }

  public Location getLocation() {
    return location;
  }

  public OutputStream getOutputStream() {
    return out;
  }

  public long getNumOfBytes() {
    return numOfBytes;
  }

  public long getLastWriteTimestamp() {
    return lastWriteTimestamp;
  }

  @Override
  public void write(byte[] b) throws IOException {
    out.write(b);
    long length = location.length();

    if (numOfBytes < length) {
      numOfBytes = length;
    }
    numOfBytes = numOfBytes + b.length;
    lastWriteTimestamp = System.currentTimeMillis();
  }

  @Override
  public void flush() throws IOException {
    // output stream on hdfs should be org.apache.hadoop.fs.Syncable
    if (out instanceof org.apache.hadoop.fs.Syncable) {
      ((org.apache.hadoop.fs.Syncable) out).hflush();
    } else {
      out.flush();
    }
  }

  @Override
  public void sync() throws IOException {
    // output stream on hdfs should be org.apache.hadoop.fs.Syncable
    if (out instanceof org.apache.hadoop.fs.Syncable) {
      ((org.apache.hadoop.fs.Syncable) out).hsync();
    } else {
      out.flush();
    }
  }

  @Override
  public void close() throws IOException {
    LOG.trace("Closing file {}", location);
    out.close();
  }
}
