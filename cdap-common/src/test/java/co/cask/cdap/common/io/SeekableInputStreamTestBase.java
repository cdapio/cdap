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

package co.cask.cdap.common.io;

import com.google.common.io.InputSupplier;
import org.apache.hadoop.fs.Syncable;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

/**
 * Unit tests for {@link SeekableInputStream}.
 */
public abstract class SeekableInputStreamTestBase {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  protected abstract LocationFactory getLocationFactory();

  @Test
  public void testClosedStream() throws IOException {
    Location location = getLocationFactory().create("testClosed");

    byte[] bytes = new byte[1024];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) (i & 0xff);
    }

    // Writes 1024 bytes to the output, and close the stream
    OutputStream output = Locations.newOutputSupplier(location).getOutput();
    output.write(bytes);
    output.close();

    // Create a SeekableInputStream for the location
    InputSupplier<? extends SeekableInputStream> inputSupplier = Locations.newInputSupplier(location);
    SeekableInputStream input = inputSupplier.getInput();

    // The stream size should be 1024
    Assert.assertEquals(bytes.length, input.size());

    // Seek to some random location and verify the byte read
    Random random = new Random();
    for (int i = 0; i < 100; i++) {
      long pos = random.nextInt(bytes.length);
      input.seek(pos);
      Assert.assertEquals(pos % 256, input.read());
    }
    input.close();
  }

  @Test
  public void testLiveStream() throws IOException {
    Location location = getLocationFactory().create("testSeekable");

    byte[] bytes = new byte[1024];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) (i & 0xff);
    }

    // Writes 1024 bytes to the output, and keep the output stream open
    OutputStream output = Locations.newOutputSupplier(location).getOutput();
    output.write(bytes);
    sync(output);

    // Create a SeekableInputStream for the location
    InputSupplier<? extends SeekableInputStream> inputSupplier = Locations.newInputSupplier(location);
    SeekableInputStream input = inputSupplier.getInput();

    // The stream size should be 1024
    Assert.assertEquals(bytes.length, input.size());

    // Seek to some random location and verify the byte read
    Random random = new Random();
    for (int i = 0; i < 100; i++) {
      long pos = random.nextInt(bytes.length);
      input.seek(pos);
      Assert.assertEquals(pos % 256, input.read());
    }

    input.close();

    // Write another 1024 bytes and keep the output stream open
    output.write(bytes);
    sync(output);

    // Reopen the input stream
    input = inputSupplier.getInput();

    // The stream size should be 2048
    Assert.assertEquals(bytes.length * 2, input.size());

    // Seek to some random location and verify the byte read
    for (int i = 0; i < 100; i++) {
      long pos = random.nextInt(bytes.length * 2);
      input.seek(pos);
      Assert.assertEquals(pos % 256, input.read());
    }

    output.close();
    input.close();
  }

  private void sync(OutputStream output) throws IOException {
    if (output instanceof Syncable) {
      ((Syncable) output).hsync();
    } else {
      output.flush();
    }
  }
}
