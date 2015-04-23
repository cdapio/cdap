/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers.log;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.LoggingEvent;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.ApplicationLoggingContext;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.logging.context.FlowletLoggingContext;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.context.MapReduceLoggingContext;
import co.cask.cdap.logging.context.UserServiceLoggingContext;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.read.Callback;
import co.cask.cdap.logging.read.LogEvent;
import co.cask.cdap.logging.read.LogOffset;
import co.cask.cdap.logging.read.LogReader;
import co.cask.cdap.logging.read.ReadRange;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
* Mock LogReader for testing.
*/
public class MockLogReader implements LogReader {
  private static final Logger LOG = LoggerFactory.getLogger(MockLogReader.class);

  private final List<LogEvent> logEvents = Lists.newArrayList();
  private static final int MAX = 80;
  public static final String TEST_NAMESPACE = "testNamespace";

  MockLogReader() {
    // Add logs for app testApp1, flow testFlow1
    generateLogs(new FlowletLoggingContext(Constants.DEFAULT_NAMESPACE,
                                           "testApp1", "testFlow1", "testFlowlet1", "", ""));

    // Add logs for app testApp3, mapreduce testMapReduce1
    generateLogs(new MapReduceLoggingContext(Constants.DEFAULT_NAMESPACE,
                                             "testApp3", "testMapReduce1", "", null));

    // Add logs for app testApp1, service testService1
    generateLogs(new UserServiceLoggingContext(Constants.DEFAULT_NAMESPACE,
                                               "testApp4", "testService1", "test1", "", ""));

    // Add logs for app testApp1, mapreduce testMapReduce1 run as part of batch adapter adapter1 in testNamespace
    generateLogs(new MapReduceLoggingContext(TEST_NAMESPACE,
                                             "testTemplate1", "testMapReduce1", "", "testAdapter1"));

    // Add logs for app testApp1, flow testFlow1 in testNamespace
    generateLogs(new FlowletLoggingContext(TEST_NAMESPACE,
                                           "testApp1", "testFlow1", "testFlowlet1", "", ""));

    // Add logs for app testApp1, service testService1 in testNamespace
    generateLogs(new UserServiceLoggingContext(TEST_NAMESPACE,
                                               "testApp4", "testService1", "test1", "", ""));
  }

  @Override
  public void getLogNext(LoggingContext loggingContext, ReadRange readRange, int maxEvents, Filter filter,
                         Callback callback) {
    if (readRange.getKafkaOffset() < 0) {
      getLogPrev(loggingContext, readRange, maxEvents, filter, callback);
      return;
    }

    Filter contextFilter = LoggingContextHelper.createFilter(loggingContext);

    callback.init();
    try {
      int count = 0;
      for (LogEvent logLine : logEvents) {
        if (logLine.getOffset().getKafkaOffset() >= readRange.getKafkaOffset()) {
          if (!contextFilter.match(logLine.getLoggingEvent())) {
            continue;
          }

          if (++count > maxEvents) {
            break;
          }

          if (filter != Filter.EMPTY_FILTER && logLine.getOffset().getKafkaOffset() % 2 != 0) {
            continue;
          }

          callback.handle(logLine);
        }
      }
    } catch (Throwable e) {
      LOG.error("Got exception", e);
    } finally {
      callback.close();
    }
  }

  @Override
  public void getLogPrev(LoggingContext loggingContext, ReadRange readRange, int maxEvents, Filter filter,
                         Callback callback) {
    if (readRange.getKafkaOffset() < 0) {
      readRange = ReadRange.createToRange(new LogOffset(MAX, MAX));
    }

    Filter contextFilter = LoggingContextHelper.createFilter(loggingContext);

    callback.init();
    try {
      int count = 0;
      long startOffset = readRange.getKafkaOffset() - maxEvents;
      for (LogEvent logLine : logEvents) {
        if (!contextFilter.match(logLine.getLoggingEvent())) {
          continue;
        }

        if (logLine.getOffset().getKafkaOffset() >= startOffset &&
          logLine.getOffset().getKafkaOffset() < readRange.getKafkaOffset()) {
          if (++count > maxEvents) {
            break;
          }

          if (filter != Filter.EMPTY_FILTER && logLine.getOffset().getKafkaOffset() % 2 != 0) {
            continue;
          }

          callback.handle(logLine);
        }
      }
    } catch (Throwable e) {
      LOG.error("Got exception", e);
    } finally {
      callback.close();
    }
  }

  @Override
  public void getLog(LoggingContext loggingContext, long fromTimeMs, long toTimeMs, Filter filter,
                     Callback callback) {
    long fromOffset = fromTimeMs / 1000;
    getLogNext(loggingContext, ReadRange.createFromRange(new LogOffset(fromOffset, fromOffset)),
               (int) (toTimeMs - fromTimeMs) / 1000, filter, callback);
  }

  private static final Function<LoggingContext.SystemTag, String> TAG_TO_STRING_FUNCTION =
    new Function<LoggingContext.SystemTag, String>() {
      @Override
      public String apply(LoggingContext.SystemTag input) {
        return input.getValue();
      }
    };

  private void generateLogs(LoggingContext loggingContext) {
    String entityId = LoggingContextHelper.getEntityId(loggingContext).getValue();
    for (int i = 0; i < MAX; ++i) {
      LoggingEvent event =
        new LoggingEvent("com.continuiity.Test",
                         (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME),
                         i % 2 == 0 ? Level.ERROR : Level.WARN, entityId + "<img>-" + i, null, null);

      // Add runid to logging context
      Map<String, String> tagMap = Maps.newHashMap(Maps.transformValues(loggingContext.getSystemTagsMap(),
                                                                         TAG_TO_STRING_FUNCTION));
      tagMap.put(ApplicationLoggingContext.TAG_RUNID_ID, String.valueOf(i % 2));
      event.setMDCPropertyMap(tagMap);
      logEvents.add(new LogEvent(event, new LogOffset(i, i)));
    }
  }
}
