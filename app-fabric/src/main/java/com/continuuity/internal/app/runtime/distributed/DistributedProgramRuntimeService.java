/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.app.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.queue.QueueSpecification;
import com.continuuity.app.queue.QueueSpecificationGenerator;
import com.continuuity.app.runtime.AbstractProgramRuntimeService;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramResourceReporter;
import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.internal.app.program.TypeId;
import com.continuuity.internal.app.queue.SimpleQueueSpecificationGenerator;
import com.continuuity.internal.app.runtime.AbstractResourceReporter;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.flow.FlowUtils;
import com.continuuity.internal.app.runtime.service.LiveInfo;
import com.continuuity.internal.app.runtime.service.NotRunningLiveInfo;
import com.continuuity.internal.app.runtime.service.SimpleRuntimeInfo;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.TwillRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.continuuity.internal.app.runtime.distributed.Containers.ContainerInfo;
import static com.continuuity.internal.app.runtime.distributed.Containers.ContainerType.FLOWLET;

/**
 *
 */
public final class DistributedProgramRuntimeService extends AbstractProgramRuntimeService {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedProgramRuntimeService.class);

  // Pattern to split a Twill App name into [type].[accountId].[appName].[programName]
  private static final Pattern APP_NAME_PATTERN = Pattern.compile("^(\\S+)\\.(\\S+)\\.(\\S+)\\.(\\S+)$");

  private final TwillRunner twillRunner;

  // TODO (terence): Injection of Store and QueueAdmin is a hack for queue reconfiguration.
  // Need to remove it when FlowProgramRunner can runs inside Twill AM.
  private final Store store;
  private final QueueAdmin queueAdmin;
  private final StreamAdmin streamAdmin;
  private final ProgramResourceReporter resourceReporter;


  @Inject
  DistributedProgramRuntimeService(ProgramRunnerFactory programRunnerFactory, TwillRunner twillRunner,
                                   StoreFactory storeFactory, QueueAdmin queueAdmin, StreamAdmin streamAdmin,
                                   MetricsCollectionService metricsCollectionService,
                                   Configuration hConf, CConfiguration cConf) {
    super(programRunnerFactory);
    this.twillRunner = twillRunner;
    this.store = storeFactory.create();
    this.queueAdmin = queueAdmin;
    this.streamAdmin = streamAdmin;
    this.resourceReporter = new ClusterResourceReporter(metricsCollectionService, hConf, cConf);
  }

  @Override
  public synchronized RuntimeInfo lookup(final RunId runId) {
    RuntimeInfo runtimeInfo = super.lookup(runId);
    if (runtimeInfo != null) {
      return runtimeInfo;
    }

    // Lookup all live applications and look for the one that matches runId
    String appName = null;
    TwillController controller = null;
    for (TwillRunner.LiveInfo liveInfo : twillRunner.lookupLive()) {
      for (TwillController c : liveInfo.getControllers()) {
        if (c.getRunId().equals(runId)) {
          appName = liveInfo.getApplicationName();
          controller = c;
          break;
        }
      }
      if (controller != null) {
        break;
      }
    }

    if (controller == null) {
      LOG.info("No running instance found for RunId {}", runId);
      // TODO (ENG-2623): How about mapreduce job?
      return null;
    }

    Matcher matcher = APP_NAME_PATTERN.matcher(appName);
    if (!matcher.matches()) {
      LOG.warn("Unrecognized application name pattern {}", appName);
      return null;
    }

    Type type = getType(matcher.group(1));
    if (type == null) {
      LOG.warn("Unrecognized program type {}", appName);
      return null;
    }
    Id.Program programId = Id.Program.from(matcher.group(2), matcher.group(3), matcher.group(4));

    if (runtimeInfo != null) {
      runtimeInfo = createRuntimeInfo(type, programId, controller);
      updateRuntimeInfo(type, runId, runtimeInfo);
      return runtimeInfo;
    } else {
      LOG.warn("Unable to find program {} {}", type, programId);
      return null;
    }
  }

  @Override
  public synchronized Map<RunId, RuntimeInfo> list(Type type) {
    Map<RunId, RuntimeInfo> result = Maps.newHashMap();
    result.putAll(super.list(type));

    // Goes through all live application, filter out the one that match the given type.
    for (TwillRunner.LiveInfo liveInfo : twillRunner.lookupLive()) {
      String appName = liveInfo.getApplicationName();
      Matcher matcher = APP_NAME_PATTERN.matcher(appName);
      if (!matcher.matches()) {
        continue;
      }
      Type appType = getType(matcher.group(1));
      if (appType != type) {
        continue;
      }

      for (TwillController controller : liveInfo.getControllers()) {
        RunId runId = controller.getRunId();
        if (result.containsKey(runId)) {
          continue;
        }

        Id.Program programId = Id.Program.from(matcher.group(2), matcher.group(3), matcher.group(4));
        RuntimeInfo runtimeInfo = createRuntimeInfo(type, programId, controller);
        if (runtimeInfo != null) {
          result.put(runId, runtimeInfo);
          updateRuntimeInfo(type, runId, runtimeInfo);
        } else {
          LOG.warn("Unable to find program {} {}", type, programId);
        }
      }
    }
    return ImmutableMap.copyOf(result);
  }

  private RuntimeInfo createRuntimeInfo(Type type, Id.Program programId, TwillController controller) {
    try {
      Program program = store.loadProgram(programId, type);
      Preconditions.checkNotNull(program, "Program not found");

      ProgramController programController = createController(program, controller);
      return programController == null ? null : new SimpleRuntimeInfo(programController, type, programId);
    } catch (Exception e) {
      LOG.error("Got exception: ", e);
      return null;
    }
  }

  private ProgramController createController(Program program, TwillController controller) {
    AbstractTwillProgramController programController = null;
    String programId = program.getId().getId();

    switch (program.getType()) {
      case FLOW: {
        FlowSpecification flowSpec = program.getSpecification().getFlows().get(programId);
        DistributedFlowletInstanceUpdater instanceUpdater = new DistributedFlowletInstanceUpdater(
          program, controller, queueAdmin, streamAdmin, getFlowletQueues(program, flowSpec)
        );
        programController = new FlowTwillProgramController(programId, controller, instanceUpdater);
        break;
      }
      case PROCEDURE:
        programController = new ProcedureTwillProgramController(programId, controller);
        break;
      case MAPREDUCE:
        programController = new MapReduceTwillProgramController(programId, controller);
        break;
      case WORKFLOW:
        programController = new WorkflowTwillProgramController(programId, controller);
        break;
      case WEBAPP:
        programController = new WebappTwillProgramController(programId, controller);
        break;
      case SERVICE:
        DistributedServiceRunnableInstanceUpdater instanceUpdater = new DistributedServiceRunnableInstanceUpdater(
          program, controller);
        programController = new ServiceTwillProgramController(programId, controller, instanceUpdater);
        break;
    }
    return programController == null ? null : programController.startListen();
  }

  private Type getType(String typeName) {
    try {
      return Type.valueOf(typeName.toUpperCase());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  // TODO (terence) : This method is part of the hack mentioned above. It should be removed when
  // FlowProgramRunner moved to run in AM.
  private Multimap<String, QueueName> getFlowletQueues(Program program, FlowSpecification flowSpec) {
    // Generate all queues specifications
    Id.Application appId = Id.Application.from(program.getAccountId(), program.getApplicationId());
    Table<QueueSpecificationGenerator.Node, String, Set<QueueSpecification>> queueSpecs
      = new SimpleQueueSpecificationGenerator(appId).create(flowSpec);

    // For storing result from flowletId to queue.
    ImmutableSetMultimap.Builder<String, QueueName> resultBuilder = ImmutableSetMultimap.builder();

    // Loop through each flowlet
    for (Map.Entry<String, FlowletDefinition> entry : flowSpec.getFlowlets().entrySet()) {
      String flowletId = entry.getKey();
      long groupId = FlowUtils.generateConsumerGroupId(program, flowletId);
      int instances = entry.getValue().getInstances();

      // For each queue that the flowlet is a consumer, store the number of instances for this flowlet
      for (QueueSpecification queueSpec : Iterables.concat(queueSpecs.column(flowletId).values())) {
        resultBuilder.put(flowletId, queueSpec.getQueueName());
      }
    }
    return resultBuilder.build();
  }

  @Override
  public LiveInfo getLiveInfo(Id.Program program, Type type) {
    String twillAppName = String.format("%s.%s.%s.%s", type.name().toLowerCase(),
                                      program.getAccountId(), program.getApplicationId(), program.getId());
    Iterator<TwillController> controllers = twillRunner.lookup(twillAppName).iterator();
    JsonObject json = new JsonObject();
    // this will return an empty Json if there is no live instance
    if (controllers.hasNext()) {
      TwillController controller = controllers.next();
      if (controllers.hasNext()) {
        LOG.warn("Expected at most one live instance of Twill app {} but found at least two.", twillAppName);
      }
      ResourceReport report = controller.getResourceReport();
      if (report != null) {
        DistributedLiveInfo liveInfo = new DistributedLiveInfo(program, type, report.getApplicationId());

        // if program type is flow then the container type is flowlet.
        Containers.ContainerType containerType = Type.FLOW.equals(type) ? FLOWLET :
                                                 Containers.ContainerType.valueOf(type.name());

        for (Map.Entry<String, Collection<TwillRunResources>> entry : report.getResources().entrySet()) {
          for (TwillRunResources resources : entry.getValue()) {
            liveInfo.addContainer(new ContainerInfo(containerType,
                                                    entry.getKey(),
                                                    resources.getInstanceId(),
                                                    resources.getContainerId(),
                                                    resources.getHost(),
                                                    resources.getMemoryMB(),
                                                    resources.getVirtualCores(),
                                                    resources.getDebugPort()));
          }
        }
        return liveInfo;
      }
    }
    return new NotRunningLiveInfo(program, type);
  }


  /**
   * Reports resource usage of the cluster and all the app masters of running twill programs.
   */
  private class ClusterResourceReporter extends AbstractResourceReporter {
    private static final String RM_CLUSTER_METRICS_PATH = "/ws/v1/cluster/metrics";
    private static final String CLUSTER_METRICS_CONTEXT = "-.cluster";
    private final Path hbasePath;
    private final Path continuuityPath;
    private final PathFilter continuuityFilter;
    private final String rmUrl;
    private FileSystem hdfs;

    public ClusterResourceReporter(MetricsCollectionService metricsCollectionService, Configuration hConf,
                                   CConfiguration cConf) {
      super(metricsCollectionService);
      try {
        this.hdfs = FileSystem.get(hConf);
      } catch (IOException e) {
        LOG.error("unable to get hdfs, cluster storage metrics will be unavailable");
        this.hdfs = null;
      }
      this.continuuityPath = new Path(cConf.get(Constants.CFG_HDFS_NAMESPACE));
      this.hbasePath = new Path(hConf.get(HConstants.HBASE_DIR));
      this.continuuityFilter = new ContinuuityPathFilter();
      this.rmUrl = "http://" + hConf.get(YarnConfiguration.RM_WEBAPP_ADDRESS) + RM_CLUSTER_METRICS_PATH;
    }

    @Override
    public void reportResources() {
      for (TwillRunner.LiveInfo info : twillRunner.lookupLive()) {
        String metricContext = getMetricContext(info);
        if (metricContext == null) {
          continue;
        }
        int containers = 0;
        int memory = 0;
        int vcores = 0;
        // will have multiple controllers if there are multiple runs of the same application
        for (TwillController controller : info.getControllers()) {
          ResourceReport report = controller.getResourceReport();
          if (report == null) {
            continue;
          }
          containers++;
          memory += report.getAppMasterResources().getMemoryMB();
          vcores += report.getAppMasterResources().getVirtualCores();
        }
        sendMetrics(metricContext, containers, memory, vcores);
      }
      reportClusterStorage();
      reportClusterMemory();
    }

    // YARN api is unstable, hit the webservice instead
    private void reportClusterMemory() {
      Reader reader = null;
      HttpURLConnection conn = null;
      try {
        URL url = new URL(rmUrl);
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        reader = new InputStreamReader(conn.getInputStream(), Charsets.UTF_8);
        JsonObject response = new Gson().fromJson(reader, JsonObject.class);
        if (response != null) {
          JsonObject clusterMetrics = response.getAsJsonObject("clusterMetrics");
          long totalMemory = clusterMetrics.get("totalMB").getAsLong();
          long availableMemory = clusterMetrics.get("availableMB").getAsLong();
          MetricsCollector collector = getCollector(CLUSTER_METRICS_CONTEXT);
          LOG.trace("resource manager, total memory = " + totalMemory + " available = " + availableMemory);
          collector.gauge("resources.total.memory", (int) totalMemory);
          collector.gauge("resources.available.memory", (int) availableMemory);
        } else {
          LOG.warn("unable to get resource manager metrics, cluster memory metrics will be unavailable");
        }
      } catch (IOException e) {
        LOG.error("Exception getting cluster memory from ", e);
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException e) {
            LOG.error("Exception closing reader", e);
          }
        }
        if (conn != null) {
          conn.disconnect();
        }
      }
    }

    private void reportClusterStorage() {
      try {
        ContentSummary summary = hdfs.getContentSummary(continuuityPath);
        long totalUsed = summary.getSpaceConsumed();
        long totalFiles = summary.getFileCount();
        long totalDirectories = summary.getDirectoryCount();

        // continuuity hbase tables
        for (FileStatus fileStatus : hdfs.listStatus(hbasePath, continuuityFilter)) {
          summary = hdfs.getContentSummary(fileStatus.getPath());
          totalUsed += summary.getSpaceConsumed();
          totalFiles += summary.getFileCount();
          totalDirectories += summary.getDirectoryCount();
        }

        FsStatus hdfsStatus = hdfs.getStatus();
        long storageCapacity = hdfsStatus.getCapacity();
        long storageAvailable = hdfsStatus.getRemaining();

        MetricsCollector collector = getCollector(CLUSTER_METRICS_CONTEXT);
        // TODO: metrics should support longs
        LOG.trace("total cluster storage = " + storageCapacity + " total used = " + totalUsed);
        collector.gauge("resources.total.storage", (int) (storageCapacity / 1024 / 1024));
        collector.gauge("resources.available.storage", (int) (storageAvailable / 1024 / 1024));
        collector.gauge("resources.used.storage", (int) (totalUsed / 1024 / 1024));
        collector.gauge("resources.used.files", (int) totalFiles);
        collector.gauge("resources.used.directories", (int) totalDirectories);
      } catch (IOException e) {
        LOG.warn("Exception getting hdfs metrics", e);
      }
    }

    private class ContinuuityPathFilter implements PathFilter {
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith("continuuity");
      }
    }

    private String getMetricContext(TwillRunner.LiveInfo info) {
      Matcher matcher = APP_NAME_PATTERN.matcher(info.getApplicationName());
      if (!matcher.matches()) {
        return null;
      }

      Type type = getType(matcher.group(1));
      if (type == null) {
        return null;
      }
      Id.Program programId = Id.Program.from(matcher.group(2), matcher.group(3), matcher.group(4));
      return Joiner.on(".").join(programId.getApplicationId(),
                                 TypeId.getMetricContextId(type),
                                 programId.getId());
    }
  }

  @Override
  protected void startUp() throws Exception {
    resourceReporter.start();
    LOG.debug("started distributed program runtime service");
  }

  @Override
  protected void shutDown() throws Exception {
    resourceReporter.stop();
  }
}
