/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap;

import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.MetricValue;
import co.cask.cdap.api.metrics.MetricValues;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.program.ProgramTypeMetricTag;
import co.cask.cdap.metrics.collect.LocalMetricsCollectionService;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MetricsWriteService extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsWriteService.class);
  private static final String PROGRAM_NAME = "DataPipelineWorkflow";
  private static final Map<Integer, Long> DEFAULT_TIME = ImmutableMap.of(1, 0L, 60, 0L, 3600, 0L,
                                                                         Integer.MAX_VALUE, 0L);

  private final LocalMetricsCollectionService metricsCollectionService;
  private final ScheduledExecutorService executor;
  private final int numNamespaces;
  private final int numProgramsPerNS;
  private final int numMetricsPerSecond;
  private final int numStages;
  private final int numThreads;
  private long numPublish = 0L;
  private long avgTime = 0L;
  private Map<Integer, Long> avgWriteTime;
  private Map<Integer, Long> avgReadTime;
  private long count = 0L;

  private final MetricStore metricStore;
  private final String metricType;

  public MetricsWriteService(CConfiguration cConf, LocalMetricsCollectionService metricsCollectionService,
                             MetricStore metricStore) {
    this.metricsCollectionService = metricsCollectionService;
    this.numNamespaces = cConf.getInt("metrics.write.test.namespaces.num", 10);
    this.numProgramsPerNS = cConf.getInt("metrics.write.test.programs.per.ns", 10);
    this.numMetricsPerSecond = cConf.getInt("metrics.write.test.metrics.per.second", 1000);
    this.numStages = cConf.getInt("metrics.write.test.num.stages", 50);
    this.numThreads = cConf.getInt("metrics.write.test.num.threads", 1);
    this.executor = Executors.newScheduledThreadPool(1,
                                                     Threads.createDaemonThreadFactory("metrics-write"));
    this.metricType = cConf.get("metrics.type.value", "normal");
    this.metricStore = metricStore;
    avgWriteTime = new HashMap<>();
    avgReadTime = new HashMap<>();
    avgWriteTime.putAll(DEFAULT_TIME);
    avgReadTime.putAll(DEFAULT_TIME);
  }

  @Override
  protected void runOneIteration() throws Exception {
    long oldMetricsCount = metricsCollectionService.getUserMetricsCount();
    long startTime = System.currentTimeMillis();
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    LOG.info("Starting the metrics test.");
    Map<Integer, Long> oldWrite = metricsCollectionService.getMetricsCount();
    List<Callable<Long>> callables = new ArrayList<>();
    List<ProgramRunId> programRunIds = generateProgramRunIds();
    List<MetricsContext> metricsContexts = new ArrayList<>();
    int threadCount = 0;
    for (ProgramRunId programRunId : programRunIds) {
      MetricsContext metricsContext = createProgramMetrics(programRunId);
      // simulate user metrics
      MetricsContext userContext = metricsContext.childContext(Constants.Metrics.Tag.SCOPE, "user");
      metricsContexts.add(userContext);
      if (metricsContexts.size() == programRunIds.size() / numThreads) {
        callables.add(new WriteMetricsCallble("thread" + threadCount++, metricsContexts));
        metricsContexts.clear();
      }
    }

    executor.invokeAll(callables, 23, TimeUnit.HOURS);
    executor.shutdown();
    // sleep to wait for metrics get processed
//    TimeUnit.SECONDS.sleep(1);
//    long newMetricsCount = metricsCollectionService.getUserMetricsCount();
//    Map<Integer, Long> writeHappened = new HashMap<>();
//    Map<Integer, Long> newWrite = metricsCollectionService.getMetricsCount();
//    for (Map.Entry<Integer, Long> entry : oldWrite.entrySet()) {
//      writeHappened.put(entry.getKey(), newWrite.get(entry.getKey()) - entry.getValue());
//    }
//    LOG.info("Stopped the metrics test in {} milliseconds. " +
//          "Generated {} metric value.", System.currentTimeMillis() - startTime, newMetricsCount - oldMetricsCount);
//    LOG.info("The number of level db write happened is: {}", writeHappened);
  }

  @Override
  protected ScheduledExecutorService executor() {
    return executor;
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(0, 1, TimeUnit.DAYS);
  }

  /**
   * Creates a {@link MetricsContext} for metrics emission of the program represented by this context.
   *
   * @param programRunId the {@link ProgramRunId} of the current execution
   * @return a {@link MetricsContext} for emitting metrics for the current program context.
   */
  private MetricsContext createProgramMetrics(ProgramRunId programRunId) {
    Map<String, String> tags = new HashMap<>();
    tags.put(Constants.Metrics.Tag.NAMESPACE, programRunId.getNamespace());
    tags.put(Constants.Metrics.Tag.APP, programRunId.getApplication());
    tags.put(ProgramTypeMetricTag.getTagName(programRunId.getType()), programRunId.getProgram());
    tags.put(Constants.Metrics.Tag.RUN_ID, programRunId.getRun());

    return metricsCollectionService.getContext(tags);
  }

  private List<ProgramRunId> generateProgramRunIds() {
    List<ProgramRunId> programRunIds = new ArrayList<>();
    List<NamespaceId> nsIds = new ArrayList<>();
    for (int i = 0; i < numNamespaces; i++) {
      nsIds.add(new NamespaceId("test" + i));
    }
    for (int i = 0; i < numProgramsPerNS; i++) {
      for (NamespaceId namespaceId : nsIds) {
        programRunIds.add(namespaceId.app("app" + i).workflow(PROGRAM_NAME).run(RunIds.generate()));
      }
    }
    return programRunIds;
  }

  private class WriteMetricsCallble implements Callable<Long> {
    private final List<MetricsContext> metricsContexts;
    private final String name;

    WriteMetricsCallble(String name, List<MetricsContext> metricsContexts) {
      this.name = name;
      this.metricsContexts = new ArrayList<>(metricsContexts);
    }

    @Override
    public Long call() {
      LOG.info("Starting to emit metrics for {} pipelines for thread {}", metricsContexts.size(), name);
      while (!Thread.currentThread().isInterrupted()) {
        for (MetricsContext metricsContext : metricsContexts) {
          metricsContext.resetTime();
        }
        long startTime = System.currentTimeMillis();
        emitMetrics();
        long duration = System.currentTimeMillis() - startTime;
   //     if (duration > 10) {
        //  long totalTime = 0L;
//          for (MetricsContext metricsContext : metricsContexts) {
//            totalTime += metricsContext.getTime();
//          }
//          LOG.info("Took {} ms to write {} metrics for thread {}, the time elapsed in the metrics context is: {}",
//                   duration,
//                   7 * numStages * numMetricsPerSecond * metricsContexts.size(), name, totalTime);
        }
//          // Don't sleep if sleepTime returned is 0
//          if (sleepTime > 0) {
//            TimeUnit.MILLISECONDS.sleep(sleepTime);
//          }
//        } catch (InterruptedException e) {
//          LOG.info("Emitted {} metrics for {} pipelines", count, metricsContexts.size());
//          return false;
//        }
   //   }
      LOG.info("Emitted {} metrics for {} pipelines for thread {}", count, metricsContexts.size(), name);
      return count;
    }

    private void emitMetrics() {
      long timestamp = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
      List<MetricValues> metricValues = new ArrayList<>();
      for (MetricsContext metricsContext : metricsContexts) {
        List<MetricValue> metrics = new ArrayList<>();
        for (int j = 0; j < numStages; j++) {
          String prefix = "stage" + j + ".";
          if (metricType.equals("normal")) {
            metrics.add(new MetricValue(prefix + Metrics.TOTAL_TIME, MetricType.COUNTER, 5L));
            metrics.add(new MetricValue(prefix + Metrics.MAX_TIME, MetricType.GAUGE, 5L));
            metrics.add(new MetricValue(prefix + Metrics.MIN_TIME, MetricType.GAUGE, 5L));
            metrics.add(new MetricValue(prefix + Metrics.AVG_TIME, MetricType.GAUGE, 5L));
            metrics.add(new MetricValue(prefix + Metrics.STD_DEV_TIME, MetricType.GAUGE, 5L));
            metrics.add(new MetricValue(prefix + Metrics.RECORDS_OUT, MetricType.COUNTER, 5L));
            metrics.add(new MetricValue(prefix + Metrics.RECORDS_IN, MetricType.COUNTER, 5L));
          } else if (metricType.equals("gauge")) {
            metrics.add(new MetricValue(prefix + Metrics.TOTAL_TIME, MetricType.GAUGE, 5L));
            metrics.add(new MetricValue(prefix + Metrics.MAX_TIME, MetricType.GAUGE, 5L));
            metrics.add(new MetricValue(prefix + Metrics.MIN_TIME, MetricType.GAUGE, 5L));
            metrics.add(new MetricValue(prefix + Metrics.AVG_TIME, MetricType.GAUGE, 5L));
            metrics.add(new MetricValue(prefix + Metrics.STD_DEV_TIME, MetricType.GAUGE, 5L));
            metrics.add(new MetricValue(prefix + Metrics.RECORDS_OUT, MetricType.GAUGE, 5L));
            metrics.add(new MetricValue(prefix + Metrics.RECORDS_IN, MetricType.GAUGE, 5L));
          } else {
            metrics.add(new MetricValue(prefix + Metrics.TOTAL_TIME, MetricType.COUNTER, 5L));
            metrics.add(new MetricValue(prefix + Metrics.MAX_TIME, MetricType.COUNTER, 5L));
            metrics.add(new MetricValue(prefix + Metrics.MIN_TIME, MetricType.COUNTER, 5L));
            metrics.add(new MetricValue(prefix + Metrics.AVG_TIME, MetricType.COUNTER, 5L));
            metrics.add(new MetricValue(prefix + Metrics.STD_DEV_TIME, MetricType.COUNTER, 5L));
            metrics.add(new MetricValue(prefix + Metrics.RECORDS_OUT, MetricType.COUNTER, 5L));
            metrics.add(new MetricValue(prefix + Metrics.RECORDS_IN, MetricType.COUNTER, 5L));
          }
//          // use some default values for time metrics since we want to test if the metrics system is able to write,
//          // dont care about the actual number
//          metricsContext.increment(prefix + Metrics.TOTAL_TIME, 5L);
//          metricsContext.gauge(prefix + Metrics.MAX_TIME, 10L);
//          metricsContext.gauge(prefix + Metrics.MIN_TIME, 1L);
//          metricsContext.gauge(prefix + Metrics.AVG_TIME, 5L);
//          metricsContext.gauge(prefix + Metrics.STD_DEV_TIME, 2L);
//
//          // these will be records.in and records.out, this number can be used to verify if the metrics system result
//          // is correct
//          metricsContext.increment(prefix + Metrics.RECORDS_IN, 1);
//          metricsContext.increment(prefix + Metrics.RECORDS_OUT, 1);
          count += 7;
        }
        metricValues.add(new MetricValues(metricsContext.getTags(), timestamp, metrics));
      }
      Map<Integer, Long> oldWriteTime = metricStore.getWriteTime();
      Map<Integer, Long> oldMetricsCount = metricStore.getCounts();
      Map<Integer, Long> oldWriteTimeDB = metricStore.getWriteTimeDB();
      Map<Integer, Long> oldReadTimeDB = metricStore.getReadTimeDB();
      Map<Integer, Long> oldMapSize = metricStore.getMapSizeDB();

      long startTime = System.currentTimeMillis();
      metricStore.add(metricValues);
      long duration = System.currentTimeMillis() - startTime;
      if (metricValues.size() > 50) {
        numPublish++;
        long delta = duration - avgTime;
        avgTime += delta / numPublish;

        Map<Integer, Long> newWriteTime = metricStore.getWriteTime();
        Map<Integer, Long> newMetricsCount = metricStore.getCounts();
        Map<Integer, Long> newWriteTimeDB = metricStore.getWriteTimeDB();
        Map<Integer, Long> newReadTimeDB = metricStore.getReadTimeDB();
        Map<Integer, Long> newMapSize = metricStore.getMapSizeDB();

        Map<Integer, Long> timeElapsed = new HashMap<>();
        Map<Integer, Long> writeHappened = new HashMap<>();
        Map<Integer, Long> newWriteTimeDBElapased = new HashMap<>();
        Map<Integer, Long> newReadTimeElapsed = new HashMap<>();
        Map<Integer, Long> newMapSizeHappened = new HashMap<>();

        for (Map.Entry<Integer, Long> entry : oldWriteTime.entrySet()) {
          timeElapsed.put(entry.getKey(), newWriteTime.get(entry.getKey()) - entry.getValue());
        }

        for (Map.Entry<Integer, Long> entry : oldMetricsCount.entrySet()) {
          writeHappened.put(entry.getKey(), newMetricsCount.get(entry.getKey()) - entry.getValue());
        }

        for (Map.Entry<Integer, Long> entry : oldWriteTimeDB.entrySet()) {
          newWriteTimeDBElapased.put(entry.getKey(), newWriteTimeDB.get(entry.getKey()) - entry.getValue());
        }

        for (Map.Entry<Integer, Long> entry : oldReadTimeDB.entrySet()) {
          newReadTimeElapsed.put(entry.getKey(), newReadTimeDB.get(entry.getKey()) - entry.getValue());
        }

        for (Map.Entry<Integer, Long> entry : oldMapSize.entrySet()) {
          newMapSizeHappened.put(entry.getKey(), newMapSize.get(entry.getKey()) - entry.getValue());
        }

        for (Map.Entry<Integer, Long> entry : newWriteTimeDBElapased.entrySet()) {
          long oldAvg = avgWriteTime.get(entry.getKey());
          long change = entry.getValue() - oldAvg;
          avgWriteTime.put(entry.getKey(), oldAvg + change / numPublish);
        }

        for (Map.Entry<Integer, Long> entry : newReadTimeElapsed.entrySet()) {
          long oldAvg = avgReadTime.get(entry.getKey());
          long change = entry.getValue() - oldAvg;
          avgReadTime.put(entry.getKey(), oldAvg + change / numPublish);
        }

        long avgWriteTotal = 0L;
        for (long value : avgWriteTime.values()) {
          avgWriteTotal += value;
        }

        long avgReadTotal = 0L;
        for (long value : avgReadTime.values()) {
          avgReadTotal += value;
        }

        long totalWrites = 0L;
        for (long value : newMapSizeHappened.values()) {
          totalWrites += value;
        }

        LOG.info("Added {} metrics in one publish for {} milliseconds, the time happened in metrics write " +
                   "is: {}, the number of level db writes is: {}, the time elapsed in leveldb write is {}, " +
                   "the time elapsed in leveldb read is {}, the avg write time is {}, the avg read time is {}, " +
                   "the number of writes is {}, the total avg write time is {} milliseconds, " +
                   "the total avg read time is {} milliseconds, the total write number is {}.",
                 metricValues.size(), duration, timeElapsed, writeHappened, newWriteTimeDBElapased,
                 newReadTimeElapsed, avgWriteTime, avgReadTime, newMapSizeHappened, avgWriteTotal, avgReadTotal,
                 totalWrites);
      }
    }
  }

  /**
   * Various metric constants.
   */
  public static final class Metrics {
    public static final String TOTAL_TIME = "process.time.total";
    public static final String MIN_TIME = "process.time.min";
    public static final String MAX_TIME = "process.time.max";
    public static final String STD_DEV_TIME = "process.time.stddev";
    public static final String AVG_TIME = "process.time.avg";
    public static final String RECORDS_IN = "records.in";
    public static final String RECORDS_OUT = "records.out";
    public static final String RECORDS_ERROR = "records.error";
    public static final String RECORDS_ALERT = "records.alert";
    public static final String AGG_GROUPS = "aggregator.groups";
    public static final String JOIN_KEYS = "joiner.keys";
  }
}
