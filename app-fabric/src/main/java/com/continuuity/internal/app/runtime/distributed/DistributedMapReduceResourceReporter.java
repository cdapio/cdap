package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.app.program.Program;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.internal.app.runtime.AbstractResourceReporter;
import com.continuuity.weave.api.ResourceReport;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeaveRunResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

/**
 * Reports program resource metrics for the part of the distributed MapReduce program that
 * the controller is aware of.  MapReduce jobs act differently because the controller will create an
 * application master and another container as a mapred client that will launch the actual mapred job,
 * which is itself another application with multiple containers.  This reporter only writes metrics
 * for the first app master and the mapred client.  The mapred client will write metrics for the mapred job itself.
 */
public class DistributedMapReduceResourceReporter extends AbstractResourceReporter {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedMapReduceResourceReporter.class);
  private final WeaveController controller;

  public DistributedMapReduceResourceReporter(Program program, MetricsCollectionService collectionService,
                                              WeaveController controller) {
    super(program, collectionService);
    this.controller = controller;
  }

  /**
   * Only includes app master and mapred client, not resource usage from actual mapred job.
   */
  @Override
  public void reportResources() {
    ResourceReport report = controller.getResourceReport();
    if (report == null) {
      LOG.info("app master returned a null resource report, program probably not started yet");
      return;
    }
    WeaveRunResources appMasterResources = report.getAppMasterResources();
    int containers = 1;
    int memory = appMasterResources.getMemoryMB();
    int vcores = appMasterResources.getVirtualCores();
    for (Collection<WeaveRunResources> weaveRunResources : report.getResources().values()) {
      containers += weaveRunResources.size();
      for (WeaveRunResources resources : weaveRunResources) {
        memory += resources.getMemoryMB();
        vcores += resources.getVirtualCores();
      }
    }
    sendMetrics(metricContextBase, containers, memory, vcores);
  }
}
