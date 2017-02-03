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

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Location outputstream used by {@link LocationManager}
 */
public class LocationOutputStream implements Flushable, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(LocationOutputStream.class);

  private Location location;
  private OutputStream outputStream;

  public LocationOutputStream(Location location, OutputStream outputStream) {
    this.location = location;
    this.outputStream = outputStream;
  }

  public Location getLocation() {
    return location;
  }

  public OutputStream getOutputStream() {
    return outputStream;
  }

  public void setLocation(Location location) {
    this.location = location;
  }

  public void setOutputStream(OutputStream outputStream) {
    this.outputStream = outputStream;
  }

  @Override
  public void flush() throws IOException {
    outputStream.flush();
    if (outputStream instanceof Syncable) {
      ((Syncable) outputStream).hsync();
    }
  }

  @Override
  public void close() throws IOException {
    LOG.trace("Closing file {}", location);
    this.flush();
    outputStream.close();
  }
}
