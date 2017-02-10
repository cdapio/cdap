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

import org.apache.hadoop.fs.Syncable;
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
public class LocationOutputStream extends FilterOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(LocationOutputStream.class);

  private Location location;
  private long numOfBytes;

  public LocationOutputStream(Location location, OutputStream outputStream) {
    super(outputStream);
    this.location = location;
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

  @Override
  public void write(byte[] b) throws IOException {
    out.write(b);
    long length = location.length();

    if (numOfBytes < length) {
      numOfBytes = length;
    }
    numOfBytes = numOfBytes + b.length;
  }

  @Override
  public void flush() throws IOException {
    out.flush();
    if (out instanceof Syncable) {
      ((Syncable) out).hsync();
    }
  }

  @Override
  public void close() throws IOException {
    LOG.trace("Closing file {}", location);
    out.close();
  }
}
