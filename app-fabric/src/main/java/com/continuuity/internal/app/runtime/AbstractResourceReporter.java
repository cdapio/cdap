package com.continuuity.internal.app.runtime;

import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.ProgramResourceReporter;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.metrics.MetricsScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Abstract implementation of a {@link com.continuuity.app.runtime.ProgramResourceReporter} that spins up a single
 * thread that will periodically report resource metrics.
 */
public abstract class AbstractResourceReporter implements ProgramResourceReporter {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramResourceReporter.class);

  private boolean running = false;
  private ExecutorService executor;
  private final MetricsCollectionService collectionService;

  protected final String metricContextBase;
  protected static final String METRIC_CONTAINERS = "resources.used.containers";
  protected static final String METRIC_MEMORY_USAGE = "resources.used.memory";
  protected static final String METRIC_VIRTUAL_CORE_USAGE = "resources.used.vcores";

  protected AbstractResourceReporter(Program program, MetricsCollectionService collectionService) {
    this.metricContextBase = getMetricContextBase(program);
    this.collectionService = collectionService;
  }

  @Override
  public void start() {
    if (running) {
      return;
    }
    running = true;
    executor = Executors.newFixedThreadPool(1);
    executor.execute(new ReporterThread());
    LOG.info("resource reporter started");
  }

  @Override
  public void stop() {
    running = false;
    executor.shutdown();
    try {
      executor.awaitTermination(10L, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.error("unable to shutdown resource reporter thread", e);
      executor.shutdownNow();
    }
  }

  /**
   * Thread that sleeps for a second then reports metrics.
   */
  private class ReporterThread extends Thread {
    @Override
    public void run() {
      while (running) {
        try {
          reportResources();
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          LOG.info("resource reporter thread interrupted");
        }
      }
    }
  }

  protected void sendAppMasterMetrics(int memory, int vcores) {
    sendMetrics(metricContextBase, 1, memory, vcores);
  }

  protected void sendMetrics(String context, int containers, int memory, int vcores) {
    LOG.debug("reporting resources in context {}: (containers, memory, vcores) = ({}, {}, {})",
              context, containers, memory, vcores);
    MetricsCollector collector = collectionService.getCollector(MetricsScope.REACTOR, context, "0");
    collector.gauge(METRIC_CONTAINERS, containers);
    collector.gauge(METRIC_MEMORY_USAGE, memory);
    collector.gauge(METRIC_VIRTUAL_CORE_USAGE, vcores);
  }

  /**
   * Returns the metric context base.  A metric context is of the form
   * {applicationId}.{programTypeId}.{programId}.{componentId}.  So for flows, it will look like
   * appX.f.flowY.flowletZ.  For procedures, appX.p.procedureY.  For mapreduce jobs, appX.b.mapredY.{mapper | reducer}.
   * This function returns the base of the context, which is the part before the final '.' separator.
   */
  private String getMetricContextBase(Program program) {
    String base = program.getApplicationId() + "." + program.getType().getMetricId();
    switch (program.getType()) {
      case FLOW:
      case MAPREDUCE:
        base += "." + program.getName();
        break;
      default:
        break;
    }
    return base;
  }
}
