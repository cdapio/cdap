/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.logging.appender.system;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.LoggingEvent;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link LogFileOutputStream}.
 */
public class LogFileOutputStreamTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testAppendFlushSyncAndClose() throws Exception {
    LocalLocationFactory locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    Location location = locationFactory.create("test.avro");
    Assert.assertTrue(location.createNew());

    final AtomicBoolean callbackClosed = new AtomicBoolean(false);
    final LogFileOutputStream stream = new LogFileOutputStream(
        location, "", 1024, 1700000000000L, new Closeable() {
          @Override
          public void close() {
            callbackClosed.set(true);
          }
        });

    LoggingEvent event = new LoggingEvent();
    event.setLevel(Level.INFO);
    event.setLoggerName("io.cdap.Test");
    event.setMessage("hello avro");
    event.setTimeStamp(1700000000100L);

    stream.append(event);
    stream.sync();
    Assert.assertTrue(stream.getSize() > 0);
    Assert.assertEquals(1700000000000L, stream.getCreateTime());
    Assert.assertSame(location, stream.getLocation());

    stream.close();
    Assert.assertTrue(callbackClosed.get());
  }

  @Test
  public void testConstructorClosesQuietlyWhenOpenStreamFails() throws Exception {
    Location location = Mockito.mock(Location.class);
    IOException openError = new IOException("open stream failed");
    Mockito.when(location.getOutputStream()).thenThrow(openError);

    try {
      new LogFileOutputStream(location, "", 1024, 100L, () -> { });
      Assert.fail("Expected IOException when getOutputStream fails");
    } catch (IOException actual) {
      Assert.assertSame(openError, actual);
    }
  }

  @Test
  public void testConstructorClosesStreamQuietlyWhenAvroHeaderWriteAndCloseFail() throws Exception {
    final AtomicBoolean closeAttempted = new AtomicBoolean(false);
    final IOException writeError = new IOException("avro header write failed");
    OutputStream failingStream = new OutputStream() {
      @Override
      public void write(int b) throws IOException {
        throw writeError;
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        throw writeError;
      }

      @Override
      public void close() throws IOException {
        closeAttempted.set(true);
        throw new IOException("close also failed");
      }
    };

    Location location = Mockito.mock(Location.class);
    Mockito.when(location.getOutputStream("600")).thenReturn(failingStream);

    try {
      new LogFileOutputStream(location, "600", 1024, 100L, () -> { });
      Assert.fail("Expected IOException when Avro header creation fails");
    } catch (IOException actual) {
      Assert.assertSame(writeError, actual);
      Assert.assertTrue(closeAttempted.get());
    }
  }
}
