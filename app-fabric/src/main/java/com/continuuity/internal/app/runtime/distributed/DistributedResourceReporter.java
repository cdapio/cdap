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
 * Reports program resource metrics for distributed programs run using Weave.
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
    ResourceReport report = controller.getResourceReport();
    if (report == null) {
      LOG.info("app master returned a null resource report, program probably not started yet");
      return;
    }
    WeaveRunResources appMasterResources = report.getAppMasterResources();
    sendAppMasterMetrics(appMasterResources.getMemoryMB(), appMasterResources.getVirtualCores());
    for (Map.Entry<String, Collection<WeaveRunResources>> weaveRunnableEntry : report.getResources().entrySet()) {
      String weaveRunnableName = weaveRunnableEntry.getKey();
      int containers = weaveRunnableEntry.getValue().size();
      int memory = 0;
      int vcores = 0;
      for (WeaveRunResources resources : weaveRunnableEntry.getValue()) {
        memory += resources.getMemoryMB();
        vcores += resources.getVirtualCores();
      }
      sendMetrics(metricContextBase + "." + weaveRunnableName, containers, memory, vcores);
    }
  }
}
