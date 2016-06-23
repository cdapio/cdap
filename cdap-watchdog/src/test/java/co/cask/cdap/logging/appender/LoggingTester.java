/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.logging.appender;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;
import co.cask.cdap.common.logging.ApplicationLoggingContext;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.NamespaceLoggingContext;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.read.Callback;
import co.cask.cdap.logging.read.LogEvent;
import co.cask.cdap.logging.read.LogOffset;
import co.cask.cdap.logging.read.LogReader;
import co.cask.cdap.logging.read.ReadRange;
import com.google.common.base.Function;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class LoggingTester {
  public void generateLogs(Logger logger, LoggingContext loggingContextNs1) {
    Exception e1 = new Exception("Test Exception1");
    Exception e2 = new Exception("Test Exception2", e1);

    LoggingContext loggingContextNs2 =
      replaceTag(loggingContextNs1,
                 new Entry(NamespaceLoggingContext.TAG_NAMESPACE_ID, getNamespace2(loggingContextNs1)));

    LoggingContextAccessor.setLoggingContext(loggingContextNs2);
    for (int i = 0; i < 40; ++i) {
      logger.warn("NS_2 Test log message {} {} {}", i, "arg1", "arg2", e2);
    }

    LoggingContextAccessor.setLoggingContext(loggingContextNs1);
    for (int i = 0; i < 20; ++i) {
      logger.warn("Test log message {} {} {}", i, "arg1", "arg2", e2);
    }

    LoggingContextAccessor.setLoggingContext(loggingContextNs2);
    for (int i = 40; i < 80; ++i) {
      logger.warn("NS_2 Test log message {} {} {}", i, "arg1", "arg2", e2);
    }

    LoggingContextAccessor.setLoggingContext(loggingContextNs1);
    for (int i = 20; i < 40; ++i) {
      logger.warn("Test log message {} {} {}", i, "arg1", "arg2", e2);
    }

    LoggingContextAccessor.setLoggingContext(loggingContextNs1);
    for (int i = 40; i < 60; ++i) {
      logger.warn("Test log message {} {} {}", i, "arg1", "arg2", e2);
    }

    // Add logs with a different runid
    LoggingContextAccessor.setLoggingContext(
      replaceTag(loggingContextNs1, new Entry(ApplicationLoggingContext.TAG_RUN_ID, "RUN2")));
    for (int i = 40; i < 60; ++i) {
      logger.warn("RUN2 Test log message {} {} {}", i, "arg1", "arg2", e2);
    }

    // Add logs with null runid and null instanceid
    LoggingContextAccessor.setLoggingContext(
      replaceTag(loggingContextNs1, new Entry(ApplicationLoggingContext.TAG_RUN_ID, null),
                 new Entry(ApplicationLoggingContext.TAG_INSTANCE_ID, null)));
    for (int i = 40; i < 60; ++i) {
      logger.warn("NULL Test log message {} {} {}", i, "arg1", "arg2", e2);
    }

    // Check with null runId and null instanceId
    LoggingContextAccessor.setLoggingContext(
      replaceTag(loggingContextNs2, new Entry(ApplicationLoggingContext.TAG_RUN_ID, null),
                 new Entry(ApplicationLoggingContext.TAG_INSTANCE_ID, null)));
    for (int i = 80; i < 120; ++i) {
      logger.warn("NS_2 Test log message {} {} {}", i, "arg1", "arg2", e2);
    }

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    StatusPrinter.setPrintStream(new PrintStream(bos));
    StatusPrinter.print((LoggerContext) LoggerFactory.getILoggerFactory());
    System.out.println(bos.toString());
  }

  public void testGetNext(LogReader logReader, LoggingContext loggingContext) throws Exception {
    LogCallback logCallback1 = new LogCallback();
    logReader.getLogNext(loggingContext, ReadRange.LATEST, 10, Filter.EMPTY_FILTER, logCallback1);
    List<LogEvent> events = logCallback1.getEvents();
    Assert.assertEquals(10, events.size());
    Assert.assertEquals("Test log message 50 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 59 arg1 arg2", events.get(9).getLoggingEvent().getFormattedMessage());

    LogOffset ultimateOffset =  events.get(9).getOffset();
    LogOffset penultimateOffset = events.get(8).getOffset();

    LogCallback logCallback2 = new LogCallback();
    logReader.getLogPrev(loggingContext, ReadRange.createToRange(logCallback1.getFirstOffset()), 20,
                         Filter.EMPTY_FILTER, logCallback2);
    events = logCallback2.getEvents();
    Assert.assertEquals(20, events.size());
    Assert.assertEquals("Test log message 30 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 49 arg1 arg2", events.get(19).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback3 = new LogCallback();
    logReader.getLogNext(loggingContext, ReadRange.createFromRange(logCallback2.getLastOffset()), 20,
                         Filter.EMPTY_FILTER, logCallback3);
    events = logCallback3.getEvents();
    Assert.assertEquals(10, events.size());
    Assert.assertEquals("Test log message 50 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 59 arg1 arg2", events.get(9).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback4 = new LogCallback();
    logReader.getLogNext(loggingContext, ReadRange.createFromRange(logCallback2.getFirstOffset()), 20,
                         Filter.EMPTY_FILTER, logCallback4);
    events = logCallback4.getEvents();
    Assert.assertEquals(20, events.size());
    Assert.assertEquals("Test log message 31 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 50 arg1 arg2", events.get(19).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback5 = new LogCallback();
    logReader.getLogNext(loggingContext, ReadRange.createFromRange(ultimateOffset), 20, Filter.EMPTY_FILTER,
                         logCallback5);
    events = logCallback5.getEvents();
    Assert.assertEquals(0, events.size());

    LogCallback logCallback6 = new LogCallback();
    logReader.getLogNext(loggingContext, ReadRange.createFromRange(getNextOffset(ultimateOffset)), 20,
                         Filter.EMPTY_FILTER, logCallback6);
    events = logCallback6.getEvents();
    Assert.assertEquals(0, events.size());

    LogCallback logCallback7 = new LogCallback();
    logReader.getLogNext(loggingContext, ReadRange.createFromRange(penultimateOffset), 20, Filter.EMPTY_FILTER,
                         logCallback7);
    events = logCallback7.getEvents();
    Assert.assertEquals(1, events.size());
    Assert.assertEquals("Test log message 59 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());

    // Try with a different run
    LogCallback logCallback10 = new LogCallback();
    logReader.getLogPrev(replaceTag(loggingContext, new Entry(ApplicationLoggingContext.TAG_RUN_ID, "RUN2")),
                         ReadRange.LATEST, 20, Filter.EMPTY_FILTER, logCallback10);
    events = logCallback10.getEvents();
    Assert.assertEquals(20, events.size());
    Assert.assertEquals("RUN2 Test log message 40 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("RUN2 Test log message 59 arg1 arg2", events.get(19).getLoggingEvent().getFormattedMessage());

    // Try with a null runid, should return all events with or without runid
    LogCallback logCallback11 = new LogCallback();
    logReader.getLogPrev(replaceTag(loggingContext, new Entry(ApplicationLoggingContext.TAG_RUN_ID, null)),
                         ReadRange.LATEST, 35, Filter.EMPTY_FILTER, logCallback11);
    events = logCallback11.getEvents();
    Assert.assertEquals(35, events.size());
    Assert.assertEquals("RUN2 Test log message 45 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("NULL Test log message 59 arg1 arg2", events.get(34).getLoggingEvent().getFormattedMessage());
  }

  public void testGetPrev(LogReader logReader, LoggingContext loggingContext) throws Exception {
    LogCallback logCallback1 = new LogCallback();
    logReader.getLogPrev(loggingContext, ReadRange.LATEST, 10, Filter.EMPTY_FILTER, logCallback1);
    List<LogEvent> events = logCallback1.getEvents();
    Assert.assertEquals(10, events.size());
    Assert.assertEquals("Test log message 50 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 59 arg1 arg2", events.get(9).getLoggingEvent().getFormattedMessage());

    LogOffset ultimateOffset =  events.get(9).getOffset();

    LogCallback logCallback2 = new LogCallback();
    logReader.getLogPrev(loggingContext, ReadRange.createToRange(logCallback1.getFirstOffset()), 20,
                         Filter.EMPTY_FILTER, logCallback2);
    events = logCallback2.getEvents();
    Assert.assertEquals(20, events.size());
    Assert.assertEquals("Test log message 30 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 49 arg1 arg2", events.get(19).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback3 = new LogCallback();
    logReader.getLogNext(loggingContext, ReadRange.createFromRange(logCallback2.getLastOffset()), 20,
                         Filter.EMPTY_FILTER, logCallback3);
    events = logCallback3.getEvents();
    Assert.assertEquals(10, events.size());
    Assert.assertEquals("Test log message 50 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 59 arg1 arg2", events.get(9).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback4 = new LogCallback();
    logReader.getLogPrev(loggingContext, ReadRange.createToRange(logCallback2.getFirstOffset()), 15,
                         Filter.EMPTY_FILTER, logCallback4);
    events = logCallback4.getEvents();
    // In kafka mode, we'll get only 10 lines, need to run the call again.
    if (events.size() < 15) {
      LogCallback logCallback41 = new LogCallback();
      logReader.getLogPrev(loggingContext, ReadRange.createToRange(logCallback4.getFirstOffset()), 5,
                           Filter.EMPTY_FILTER, logCallback41);
      events.addAll(0, logCallback41.getEvents());
      logCallback4 = logCallback41;
    }
    Assert.assertEquals(15, events.size());
    Assert.assertEquals("Test log message 15 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 29 arg1 arg2", events.get(14).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback6 = new LogCallback();
    logReader.getLogPrev(loggingContext, ReadRange.createToRange(logCallback4.getFirstOffset()), 25,
                         Filter.EMPTY_FILTER, logCallback6);
    events = logCallback6.getEvents();
    Assert.assertEquals(15, events.size());
    Assert.assertEquals("Test log message 0 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 14 arg1 arg2", events.get(14).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback5 = new LogCallback();
    logReader.getLogPrev(loggingContext, ReadRange.createToRange(logCallback6.getFirstOffset()), 15,
                         Filter.EMPTY_FILTER, logCallback5);
    events = logCallback5.getEvents();
    Assert.assertEquals(0, events.size());

    LogCallback logCallback7 = new LogCallback();
    logReader.getLogPrev(loggingContext, ReadRange.createToRange(logCallback4.getFirstOffset()), 15,
                         Filter.EMPTY_FILTER, logCallback7);
    events = logCallback7.getEvents();
    Assert.assertEquals(15, events.size());
    Assert.assertEquals("Test log message 0 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 14 arg1 arg2", events.get(14).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback9 = new LogCallback();
    logReader.getLogPrev(loggingContext, ReadRange.createToRange(getNextOffset(ultimateOffset)), 15,
                         Filter.EMPTY_FILTER, logCallback9);
    events = logCallback9.getEvents();
    Assert.assertEquals(15, events.size());
    Assert.assertEquals("Test log message 45 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 59 arg1 arg2", events.get(14).getLoggingEvent().getFormattedMessage());

    // Try with a different run
    LogCallback logCallback10 = new LogCallback();
    logReader.getLogPrev(replaceTag(loggingContext, new Entry(ApplicationLoggingContext.TAG_RUN_ID, "RUN2")),
                         ReadRange.LATEST, 20, Filter.EMPTY_FILTER, logCallback10);
    events = logCallback10.getEvents();
    Assert.assertEquals(20, events.size());
    Assert.assertEquals("RUN2 Test log message 40 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("RUN2 Test log message 59 arg1 arg2", events.get(19).getLoggingEvent().getFormattedMessage());

    // Try with a null runid, should return all events with or without runid
    LogCallback logCallback11 = new LogCallback();
    logReader.getLogPrev(replaceTag(loggingContext, new Entry(ApplicationLoggingContext.TAG_RUN_ID, null)),
                         ReadRange.LATEST, 40, Filter.EMPTY_FILTER, logCallback11);
    events = logCallback11.getEvents();
    Assert.assertEquals(40, events.size());
    Assert.assertEquals("RUN2 Test log message 40 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("NULL Test log message 59 arg1 arg2", events.get(39).getLoggingEvent().getFormattedMessage());
  }

  /**
   * Log Call back for testing.
   */
  public static class LogCallback implements Callback {
    private LogOffset firstOffset;
    private LogOffset lastOffset;
    private List<LogEvent> events;

    @Override
    public void init() {
      events = Collections.synchronizedList(new ArrayList<LogEvent>());
    }

    @Override
    public void handle(LogEvent event) {
      if (firstOffset == null) {
        firstOffset = event.getOffset();
      }
      lastOffset = event.getOffset();
      events.add(event);
    }

    @Override
    public int getCount() {
      return events.size();
    }

    @Override
    public void close() {
    }

    public List<LogEvent> getEvents() throws Exception {
      return events;
    }

    public LogOffset getFirstOffset() {
      return firstOffset == null ? LogOffset.LATEST_OFFSET : firstOffset;
    }

    public LogOffset getLastOffset() {
      return lastOffset == null ? LogOffset.LATEST_OFFSET : lastOffset;
    }
  }

  private LogOffset getNextOffset(LogOffset offset) {
    return new LogOffset(offset.getKafkaOffset() + 1, offset.getTime() + 1);
  }

  private LoggingContext replaceTag(LoggingContext loggingContext, Entry... entries) {
    Map<String, String> tagMap =
      Maps.newHashMap(Maps.transformValues(loggingContext.getSystemTagsMap(), TAG_TO_STRING_FUNCTION));
    for (Entry entry : entries) {
      tagMap.put(entry.getKey(), entry.getValue());
    }
    return LoggingContextHelper.getLoggingContext(tagMap);
  }

  private String getNamespace2(LoggingContext loggingContext) {
    String ns = loggingContext.getSystemTagsMap().get(NamespaceLoggingContext.TAG_NAMESPACE_ID).getValue();
    return ns.substring(0, ns.length() - 1) + "2";
  }

  private static final class Entry {
    private final String key;
    private final String value;

    public Entry(String key, String value) {
      this.key = key;
      this.value = value;
    }

    public String getKey() {
      return key;
    }

    public String getValue() {
      return value;
    }
  }

  private static final Function<LoggingContext.SystemTag, String> TAG_TO_STRING_FUNCTION =
    new Function<LoggingContext.SystemTag, String>() {
      @Override
      public String apply(LoggingContext.SystemTag input) {
        return input.getValue();
      }
    };
}
