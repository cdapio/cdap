/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.report.main;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link RunMetaFileOutputStream}.
 */
public class RunMetaFileOutputStreamTest {

  @Test
  public void testConstructorClosesQuietlyWhenWriteFailsAndCloseThrows() {
    IOException writeFailure = new IOException("avro header write failed");
    AtomicBoolean streamCloseCalled = new AtomicBoolean(false);
    OutputStream failingStream = new OutputStream() {
      @Override
      public void write(int b) throws IOException {
        throw writeFailure;
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        throw writeFailure;
      }

      @Override
      public void close() throws IOException {
        streamCloseCalled.set(true);
        throw new IOException("close also failed");
      }
    };

    Location location = createLocationReturningStream(failingStream, null);
    try {
      new RunMetaFileOutputStream(location, "", 1024, 100L, () -> { });
      Assert.fail("Expected original IOException from Avro header creation");
    } catch (IOException actual) {
      Assert.assertSame(writeFailure, actual);
      Assert.assertTrue(streamCloseCalled.get());
    }
  }

  @Test
  public void testConstructorRethrowsOriginalIoExceptionWhenGetOutputStreamFails() {
    IOException openFailure = new IOException("open output stream failed");
    Location location = createLocationReturningStream(null, openFailure);
    try {
      new RunMetaFileOutputStream(location, "", 1024, 100L, () -> { });
      Assert.fail("Expected IOException when opening output stream fails");
    } catch (IOException actual) {
      Assert.assertSame(openFailure, actual);
    }
  }

  private Location createLocationReturningStream(OutputStream stream, IOException openFailure) {
    return (Location) Proxy.newProxyInstance(
        Location.class.getClassLoader(),
        new Class<?>[]{Location.class},
        (proxy, method, args) -> {
          if ("getOutputStream".equals(method.getName())) {
            if (openFailure != null) {
              throw openFailure;
            }
            return stream;
          }
          if ("toString".equals(method.getName())) {
            return "mock://location";
          }
          return null;
        });
  }
}
