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

package io.cdap.cdap.logging.write;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.LoggingEvent;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.logging.filter.Filter;
import io.cdap.cdap.logging.read.LogEvent;
import io.cdap.cdap.logging.serialize.LogSchema;
import io.cdap.cdap.logging.serialize.LoggingEventSerializer;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.impersonation.Impersonator;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.concurrent.Callable;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link LogLocation} reading and {@code LocationSeekableInput} exception
 * propagation.
 */
public class LogLocationTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testReadLogAndReadLogPrevForVersion1() throws Exception {
    LocalLocationFactory locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    Location avroFile = locationFactory.create("events.avro");
    LoggingEventSerializer serializer = new LoggingEventSerializer();
    try (OutputStream out = avroFile.getOutputStream();
        DataFileWriter<GenericRecord> writer = new DataFileWriter<>(
            new GenericDatumWriter<GenericRecord>(LogSchema.LoggingEvent.SCHEMA))) {
      writer.create(LogSchema.LoggingEvent.SCHEMA, out);
      LoggingEvent e1 = new LoggingEvent();
      e1.setLevel(Level.INFO);
      e1.setLoggerName("io.cdap.Test");
      e1.setMessage("msg-1");
      e1.setTimeStamp(1000L);
      writer.append(serializer.toGenericRecord(e1));
    }

    Impersonator impersonator = Mockito.mock(Impersonator.class);
    LogLocation logLocation = new LogLocation(
        LogLocation.VERSION_1, 1000L, 900L, avroFile, "default", impersonator);

    try (CloseableIterator<LogEvent> iter =
        logLocation.readLog(Filter.EMPTY_FILTER, 0L, 2000L, 10)) {
      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals("msg-1", iter.next().getLoggingEvent().getMessage());
      Assert.assertFalse(iter.hasNext());
    }

    Collection<LogEvent> prevEvents = logLocation.readLogPrev(Filter.EMPTY_FILTER, 2000L, 10);
    Assert.assertEquals(1, prevEvents.size());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testVersion0ImpersonatorExceptionPropagation() throws Exception {
    LocalLocationFactory locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    Location avroFile = locationFactory.create("v0.avro");
    Assert.assertTrue(avroFile.createNew());

    // 1. Unchecked RuntimeException from impersonator.doAs is NOT double-wrapped
    final IllegalStateException uncheckedUgiFailure =
        new IllegalStateException("impersonation runtime failure");
    Impersonator runtimeFailingImpersonator = Mockito.mock(Impersonator.class);
    Mockito.when(runtimeFailingImpersonator.doAs(
        Mockito.eq(NamespaceId.DEFAULT), Mockito.any(Callable.class)))
        .thenThrow(uncheckedUgiFailure);

    LogLocation runtimeFailingLocation = new LogLocation(
        LogLocation.VERSION_0, 1000L, 900L, avroFile, "default", runtimeFailingImpersonator);
    try {
      runtimeFailingLocation.readLogPrev(Filter.EMPTY_FILTER, 2000L, 10);
      Assert.fail("Expected IllegalStateException without double-wrapping");
    } catch (IllegalStateException actual) {
      Assert.assertSame(uncheckedUgiFailure, actual);
    }

    // 2. Checked non-IOException Exception from impersonator.doAs is wrapped in RuntimeException
    final Exception checkedUgiFailure = new Exception("checked kerberos error");
    Impersonator checkedFailingImpersonator = Mockito.mock(Impersonator.class);
    Mockito.when(checkedFailingImpersonator.doAs(
        Mockito.eq(NamespaceId.DEFAULT), Mockito.any(Callable.class)))
        .thenThrow(checkedUgiFailure);

    LogLocation checkedFailingLocation = new LogLocation(
        LogLocation.VERSION_0, 1000L, 900L, avroFile, "default", checkedFailingImpersonator);
    try {
      checkedFailingLocation.readLogPrev(Filter.EMPTY_FILTER, 2000L, 10);
      Assert.fail("Expected RuntimeException wrapping checked Exception");
    } catch (RuntimeException actual) {
      Assert.assertEquals(RuntimeException.class, actual.getClass());
      Assert.assertSame(checkedUgiFailure, actual.getCause());
    }

    // 3. Checked IOException is rethrown as IOException in LocationSeekableInput and handled
    //    gracefully in readLogPrev
    Impersonator ioFailingImpersonator = Mockito.mock(Impersonator.class);
    Mockito.when(ioFailingImpersonator.doAs(
        Mockito.eq(NamespaceId.DEFAULT), Mockito.any(Callable.class)))
        .thenThrow(new IOException("hdfs unreachable"));
    LogLocation ioFailingLocation = new LogLocation(
        LogLocation.VERSION_0, 1000L, 900L, avroFile, "default", ioFailingImpersonator);
    Assert.assertTrue(ioFailingLocation.readLogPrev(Filter.EMPTY_FILTER, 2000L, 10).isEmpty());
  }
}
