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

import com.google.common.io.ByteStreams;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Unit test for {@link SeekableInputStream}.
 */
public abstract class SeekableInputStreamTestBase {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  protected abstract LocationFactory getLocationFactory();

  @Test
  public void testSeek() throws IOException {
    Location location = getLocationFactory().create("testSeek");

    // Writes 100 bytes to the location
    byte[] bytes = new byte[100];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) i;
    }
    ByteStreams.write(bytes, Locations.newOutputSupplier(location));

    // Create a seekable input form the location
    SeekableInputStream input = Locations.newInputSupplier(location).getInput();
    try {
      // Seek and read from different positions
      for (int i = 0; i < 100; i += 15) {
        input.seek(i);
        Assert.assertEquals(i, input.read());
      }

      // Seek and read from different -ve positions
      for (int i = 1; i < 100; i += 15) {
        input.seek(-i);
        Assert.assertEquals(100 - i, input.read());
      }

      // See beyond the length should result in IOException
      try {
        input.seek(bytes.length + 1);
        Assert.fail();
      } catch (IOException e) {
        // OK
      }

    } finally {
      input.close();
    }
  }
}
