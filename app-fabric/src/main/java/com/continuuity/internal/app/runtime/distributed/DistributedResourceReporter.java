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
 * Reports program resource metrics for distributed programs that run using Weave.
 */
public class DistributedResourceReporter extends AbstractResourceReporter {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedResourceReporter.class);
  private final WeaveController controller;

  public DistributedResourceReporter(Program program, MetricsCollectionService collectionService,
                                     WeaveController controller) {
    super(program, collectionService);
    this.controller = controller;
  }

  @Override
  public void reportResources() {
    // this will query the application master for a snapshot of resources that are in use by the application.
    ResourceReport report = controller.getResourceReport();
    if (report == null) {
      LOG.info("app master returned a null resource report, program probably not started yet");
      return;
    }
    // every flow or procedure will spin up an application master in charge of talking to the resource manager
    // and node managers to spin up the containers that do the actual work.
    WeaveRunResources appMasterResources = report.getAppMasterResources();
    sendAppMasterMetrics(appMasterResources.getMemoryMB(), appMasterResources.getVirtualCores());

    // for flows, the runnables here are instances of flowlets.  For procedures, they are instances of procedures.
    for (Map.Entry<String, Collection<WeaveRunResources>> weaveRunnableEntry : report.getResources().entrySet()) {
      String weaveRunnableName = weaveRunnableEntry.getKey();
      int containers = weaveRunnableEntry.getValue().size();
      int memory = 0;
      int vcores = 0;
      for (WeaveRunResources resources : weaveRunnableEntry.getValue()) {
        memory += resources.getMemoryMB();
        vcores += resources.getVirtualCores();
      }
      // send total containers, memory, and virtual cores used by this flowlet or procedure.
      sendMetrics(metricContextBase + "." + weaveRunnableName, containers, memory, vcores);
    }
  }
}
