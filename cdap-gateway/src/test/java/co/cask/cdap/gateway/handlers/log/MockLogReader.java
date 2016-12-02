/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.logging.ApplicationLoggingContext;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.logging.context.FlowletLoggingContext;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.context.MapReduceLoggingContext;
import co.cask.cdap.logging.context.UserServiceLoggingContext;
import co.cask.cdap.logging.context.WorkflowLoggingContext;
import co.cask.cdap.logging.context.WorkflowProgramLoggingContext;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.read.Callback;
import co.cask.cdap.logging.read.LogEvent;
import co.cask.cdap.logging.read.LogOffset;
import co.cask.cdap.logging.read.LogReader;
import co.cask.cdap.logging.read.ReadRange;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.test.SlowTests;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
* Mock LogReader for testing.
*/
@Category(SlowTests.class)
public class MockLogReader implements LogReader {
  private static final Logger LOG = LoggerFactory.getLogger(MockLogReader.class);

  public static final String TEST_NAMESPACE = "testNamespace";
  public static final NamespaceId TEST_NAMESPACE_ID = new NamespaceId(TEST_NAMESPACE);
  public static final ApplicationId SOME_WORKFLOW_APP = TEST_NAMESPACE_ID.app("someWorkflowApp");
  public static final String SOME_WORKFLOW = "someWorkflow";
  public static final String SOME_MAPREDUCE = "someMapReduce";
  public static final String SOME_SPARK = "someSpark";
  private static final int MAX = 80;

  private final DefaultStore store;
  private final List<LogEvent> logEvents = Lists.newArrayList();
  private final Map<ProgramId, RunRecord> runRecordMap = Maps.newHashMap();

  @Inject
  MockLogReader(DefaultStore store) {
    this.store = store;
  }

  public void generateLogs() throws InterruptedException {
    // Add logs for app testApp2, flow testFlow1
    generateLogs(new FlowletLoggingContext(NamespaceId.DEFAULT.getEntityName(),
                                           "testApp2", "testFlow1", "testFlowlet1", "", ""),
                 NamespaceId.DEFAULT.app("testApp2").flow("testFlow1"),
                 ProgramRunStatus.RUNNING);

    // Add logs for app testApp3, mapreduce testMapReduce1
    generateLogs(new MapReduceLoggingContext(NamespaceId.DEFAULT.getEntityName(),
                                             "testApp3", "testMapReduce1", ""),
                 NamespaceId.DEFAULT.app("testApp3").mr("testMapReduce1"),
                 ProgramRunStatus.SUSPENDED);

    // Add logs for app testApp1, service testService1
    generateLogs(new UserServiceLoggingContext(NamespaceId.DEFAULT.getEntityName(),
                                               "testApp4", "testService1", "test1", "", ""),
                 NamespaceId.DEFAULT.app("testApp4").service("testService1"),
                 ProgramRunStatus.RUNNING);

    // Add logs for app testApp1, mapreduce testMapReduce1
    generateLogs(new MapReduceLoggingContext(TEST_NAMESPACE_ID.getNamespace(),
                                             "testTemplate1", "testMapReduce1", ""),
                 TEST_NAMESPACE_ID.app("testTemplate1").mr("testMapReduce1"),
                 ProgramRunStatus.COMPLETED);

    // Add logs for app testApp1, flow testFlow1 in testNamespace
    generateLogs(new FlowletLoggingContext(TEST_NAMESPACE_ID.getNamespace(),
                                           "testApp1", "testFlow1", "testFlowlet1", "", ""),
                 TEST_NAMESPACE_ID.app("testApp1").flow("testFlow1"),
                 ProgramRunStatus.COMPLETED);

    // Add logs for app testApp1, service testService1 in testNamespace
    generateLogs(new UserServiceLoggingContext(TEST_NAMESPACE_ID.getNamespace(),
                                               "testApp4", "testService1", "test1", "", ""),
                 TEST_NAMESPACE_ID.app("testApp4").service("testService1"),
                 ProgramRunStatus.KILLED);

    // Add logs for testWorkflow1 in testNamespace
    generateLogs(new WorkflowLoggingContext(TEST_NAMESPACE_ID.getNamespace(),
                                            "testTemplate1", "testWorkflow1", "testRun1"),
                 TEST_NAMESPACE_ID.app("testTemplate1").workflow("testWorkflow1"),
                 ProgramRunStatus.COMPLETED);
    // Add logs for testWorkflow1 in default namespace
    generateLogs(new WorkflowLoggingContext(NamespaceId.DEFAULT.getEntityName(),
                                            "testTemplate1", "testWorkflow1", "testRun2"),
                 NamespaceId.DEFAULT.app("testTemplate1").workflow("testWorkflow1"),
                 ProgramRunStatus.COMPLETED);

    generateWorkflowLogs();
  }

  /**
   * Generate Workflow logs.
   */
  private void generateWorkflowLogs() {
    ProgramId workflowId = SOME_WORKFLOW_APP.workflow(SOME_WORKFLOW);
    long currentTime = TimeUnit.SECONDS.toMillis(10);
    RunId workflowRunId = RunIds.generate();
    store.setStart(workflowId, workflowRunId.getId(), currentTime);
    runRecordMap.put(workflowId, store.getRun(workflowId, workflowRunId.getId()));
    WorkflowLoggingContext wfLoggingContext = new WorkflowLoggingContext(workflowId.getNamespace(),
                                                                         workflowId.getApplication(),
                                                                         workflowId.getProgram(),
                                                                         workflowRunId.getId());
    generateWorkflowRunLogs(wfLoggingContext);

    // Generate logs for MapReduce program started by above Workflow run
    ProgramId mapReduceId = SOME_WORKFLOW_APP.mr(SOME_MAPREDUCE);
    currentTime = TimeUnit.SECONDS.toMillis(20);
    RunId mapReduceRunId = RunIds.generate();
    Map<String, String> systemArgs = ImmutableMap.of(ProgramOptionConstants.WORKFLOW_NODE_ID, SOME_MAPREDUCE,
                                                     ProgramOptionConstants.WORKFLOW_NAME, SOME_WORKFLOW,
                                                     ProgramOptionConstants.WORKFLOW_RUN_ID, workflowRunId.getId());

    store.setStart(mapReduceId, mapReduceRunId.getId(), currentTime, null, new HashMap<String, String>(),
                   systemArgs);

    runRecordMap.put(mapReduceId, store.getRun(mapReduceId, mapReduceRunId.getId()));
    WorkflowProgramLoggingContext context = new WorkflowProgramLoggingContext(workflowId.getNamespace(),
                                                                              workflowId.getApplication(),
                                                                              workflowId.getProgram(),
                                                                              workflowRunId.getId(),
                                                                              ProgramType.MAPREDUCE, SOME_MAPREDUCE,
                                                                              mapReduceRunId.getId());
    generateWorkflowRunLogs(context);

    // Generate logs for Spark program started by Workflow run above
    ProgramId sparkId = SOME_WORKFLOW_APP.spark(SOME_SPARK);
    currentTime = TimeUnit.SECONDS.toMillis(40);
    RunId sparkRunId = RunIds.generate();
    systemArgs = ImmutableMap.of(ProgramOptionConstants.WORKFLOW_NODE_ID, SOME_SPARK,
                                 ProgramOptionConstants.WORKFLOW_NAME, SOME_WORKFLOW,
                                 ProgramOptionConstants.WORKFLOW_RUN_ID, workflowRunId.getId());

    store.setStart(sparkId, sparkRunId.getId(), currentTime, null, new HashMap<String, String>(), systemArgs);
    runRecordMap.put(sparkId, store.getRun(sparkId, sparkRunId.getId()));
    context = new WorkflowProgramLoggingContext(workflowId.getNamespace(), workflowId.getApplication(),
                                                workflowId.getProgram(), workflowRunId.getId(), ProgramType.SPARK,
                                                SOME_SPARK, sparkRunId.getId());
    generateWorkflowRunLogs(context);

    // Generate some more logs for Workflow
    generateWorkflowRunLogs(wfLoggingContext);
  }

  RunRecord getRunRecord(ProgramId programId) {
    return runRecordMap.get(programId);
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
          long logTime = logLine.getLoggingEvent().getTimeStamp();
          if (!contextFilter.match(logLine.getLoggingEvent()) || logTime < readRange.getFromMillis() ||
            logTime >= readRange.getToMillis()) {
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
      readRange = new ReadRange(readRange.getFromMillis(), readRange.getToMillis(), MAX);
    }

    Filter contextFilter = LoggingContextHelper.createFilter(loggingContext);

    callback.init();
    try {
      int count = 0;
      long startOffset = readRange.getKafkaOffset() - maxEvents;
      for (LogEvent logLine : logEvents) {
        long logTime = logLine.getLoggingEvent().getTimeStamp();
        if (!contextFilter.match(logLine.getLoggingEvent()) || logTime < readRange.getFromMillis() ||
          logTime >= readRange.getToMillis()) {
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
  public CloseableIterator<LogEvent> getLog(LoggingContext loggingContext, long fromTimeMs, long toTimeMs,
                                            Filter filter) {
    CollectingCallback collectingCallback = new CollectingCallback();
    // since its just for test case, we don't need to lazily read logs (which is the purpose of returning an Iterator)
    long fromOffset = getOffset(fromTimeMs / 1000);
    long toOffset = getOffset(toTimeMs / 1000);
    getLogNext(loggingContext, new ReadRange(fromTimeMs, toTimeMs, fromOffset),
               (int) (toOffset - fromOffset), filter, collectingCallback);

    final Iterator<LogEvent> iterator = collectingCallback.getLogEvents().iterator();

    return new CloseableIterator<LogEvent>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public LogEvent next() {
        return iterator.next();
      }

      @Override
      public void remove() {
        iterator.remove();
      }

      @Override
      public void close() {
        // no-op
      }
    };

  }

  private static final Function<LoggingContext.SystemTag, String> TAG_TO_STRING_FUNCTION =
    new Function<LoggingContext.SystemTag, String>() {
      @Override
      public String apply(LoggingContext.SystemTag input) {
        return input.getValue();
      }
    };

  /**
   * Generate logs for Workflow run.
   */
  private void generateWorkflowRunLogs(LoggingContext loggingContext) {
    Logger logger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

    String programName;
    if (loggingContext instanceof WorkflowProgramLoggingContext) {
      // Logging is being done for programs running inside Workflow
      LoggingContext.SystemTag systemTag;
      systemTag = loggingContext.getSystemTagsMap().get(WorkflowProgramLoggingContext.TAG_WORKFLOW_MAP_REDUCE_ID);
      if (systemTag == null) {
        systemTag = loggingContext.getSystemTagsMap().get(WorkflowProgramLoggingContext.TAG_WORKFLOW_SPARK_ID);
      }
      programName = systemTag.getValue();
    } else {
      // Logging is done for Workflow
      programName = loggingContext.getSystemTagsMap().get(WorkflowLoggingContext.TAG_WORKFLOW_ID).getValue();
    }

    for (int i = 0; i < MAX; i++) {
      LoggingEvent event = new LoggingEvent("co.cask.Test", (ch.qos.logback.classic.Logger) logger, Level.INFO,
                                            programName + "<img>-" + i, null, null);
      Map<String, String> tagMap = Maps.newHashMap(Maps.transformValues(loggingContext.getSystemTagsMap(),
                                                                        TAG_TO_STRING_FUNCTION));
      event.setMDCPropertyMap(tagMap);
      logEvents.add(new LogEvent(event, new LogOffset(i, i)));
    }
  }

  /**
   * This method is used to generate the logs for program which are used for testing.
   * Single call to this method would add {@link #MAX} number of events.
   * First 20 events are generated without {@link ApplicationLoggingContext#TAG_RUN_ID} tag.
   * For next 40 events, alternate event is tagged with {@code ApplicationLoggingContext#TAG_RUN_ID}.
   * Last 20 events are not tagged with {@code ApplicationLoggingContext#TAG_RUN_ID}.
   * All events are alternately marked as {@link Level#ERROR} and {@link Level#WARN}.
   */
  private void generateLogs(LoggingContext loggingContext, ProgramId programId, ProgramRunStatus runStatus)
    throws InterruptedException {
    String entityId = LoggingContextHelper.getEntityId(loggingContext).getValue();
    RunId runId = null;
    Long stopTs = null;
    for (int i = 0; i < MAX; ++i) {
      // Setup run id for event with ids >= 20
      if (i == 20) {
        runId = RunIds.generate(TimeUnit.SECONDS.toMillis(getMockTimeSecs(i)));
      } else if (i == 60 && runStatus != ProgramRunStatus.RUNNING && runStatus != ProgramRunStatus.SUSPENDED) {
        // Record stop time for run for 60th event, but still continue to record run in the other logging events.
        stopTs = getMockTimeSecs(i);
      }

      LoggingEvent event =
        new LoggingEvent("co.cask.Test",
                         (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME),
                         i % 2 == 0 ? Level.ERROR : Level.WARN, entityId + "<img>-" + i, null, null);
      event.setTimeStamp(TimeUnit.SECONDS.toMillis(getMockTimeSecs(i)));

      // Add runid to logging context
      Map<String, String> tagMap = Maps.newHashMap(Maps.transformValues(loggingContext.getSystemTagsMap(),
                                                                         TAG_TO_STRING_FUNCTION));
      if (runId != null && stopTs == null && i % 2 == 0) {
        tagMap.put(ApplicationLoggingContext.TAG_RUN_ID, runId.getId());
      }
      event.setMDCPropertyMap(tagMap);
      logEvents.add(new LogEvent(event, new LogOffset(i, i)));
    }

    long startTs = RunIds.getTime(runId, TimeUnit.SECONDS);
    if (programId != null) {
      //noinspection ConstantConditions
      runRecordMap.put(programId, new RunRecord(runId.getId(), startTs, stopTs, runStatus, null));
      store.setStart(programId, runId.getId(), startTs);
      if (stopTs != null) {
        store.setStop(programId, runId.getId(), stopTs, runStatus);
      }
    }
  }

  public static long getMockTimeSecs(int offset) {
    return offset * 10;
  }

  private static long getOffset(long mockTimeSecs) {
    return mockTimeSecs / 10;
  }
 }
