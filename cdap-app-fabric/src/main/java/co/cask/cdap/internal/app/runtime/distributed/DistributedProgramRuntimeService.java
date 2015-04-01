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
package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.queue.QueueSpecification;
import co.cask.cdap.app.queue.QueueSpecificationGenerator;
import co.cask.cdap.app.runtime.AbstractProgramRuntimeService;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramResourceReporter;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.program.ProgramTypeMetricTag;
import co.cask.cdap.internal.app.queue.SimpleQueueSpecificationGenerator;
import co.cask.cdap.internal.app.runtime.AbstractResourceReporter;
import co.cask.cdap.internal.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.internal.app.runtime.flow.FlowUtils;
import co.cask.cdap.internal.app.runtime.service.SimpleRuntimeInfo;
import co.cask.cdap.proto.Containers;
import co.cask.cdap.proto.DistributedProgramLiveInfo;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NotRunningProgramLiveInfo;
import co.cask.cdap.proto.ProgramLiveInfo;
import co.cask.cdap.proto.ProgramType;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static co.cask.cdap.proto.Containers.ContainerInfo;
import static co.cask.cdap.proto.Containers.ContainerType.FLOWLET;

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
  private final TransactionExecutorFactory txExecutorFactory;
  private final ProgramResourceReporter resourceReporter;

  @Inject
  DistributedProgramRuntimeService(ProgramRunnerFactory programRunnerFactory, TwillRunner twillRunner,
                                   StoreFactory storeFactory, QueueAdmin queueAdmin, StreamAdmin streamAdmin,
                                   MetricsCollectionService metricsCollectionService,
                                   Configuration hConf, CConfiguration cConf,
                                   TransactionExecutorFactory txExecutorFactory) {
    super(programRunnerFactory);
    this.twillRunner = twillRunner;
    this.store = storeFactory.create();
    this.queueAdmin = queueAdmin;
    this.streamAdmin = streamAdmin;
    this.txExecutorFactory = txExecutorFactory;
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

    ProgramType type = getType(matcher.group(1));
    if (type == null) {
      LOG.warn("Unrecognized program type {}", appName);
      return null;
    }
    Id.Program programId = Id.Program.from(matcher.group(2), matcher.group(3), type, matcher.group(4));

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
  public synchronized Map<RunId, RuntimeInfo> list(ProgramType type) {
    Map<RunId, RuntimeInfo> result = Maps.newHashMap();
    result.putAll(super.list(type));

    // Goes through all live application, filter out the one that match the given type.
    for (TwillRunner.LiveInfo liveInfo : twillRunner.lookupLive()) {
      String appName = liveInfo.getApplicationName();
      Matcher matcher = APP_NAME_PATTERN.matcher(appName);
      if (!matcher.matches()) {
        continue;
      }
      if (!type.equals(getType(matcher.group(1)))) {
        continue;
      }

      for (TwillController controller : liveInfo.getControllers()) {
        RunId runId = controller.getRunId();
        if (result.containsKey(runId)) {
          continue;
        }

        Id.Program programId = Id.Program.from(matcher.group(2), matcher.group(3), type, matcher.group(4));
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

  private RuntimeInfo createRuntimeInfo(ProgramType type, Id.Program programId, TwillController controller) {
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
        FlowSpecification flowSpec = program.getApplicationSpecification().getFlows().get(programId);
        DistributedFlowletInstanceUpdater instanceUpdater = new DistributedFlowletInstanceUpdater(
          program, controller, queueAdmin, streamAdmin, getFlowletQueues(program, flowSpec), txExecutorFactory
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
      case WORKER:
        programController = new WorkerTwillProgramController(programId, controller);
        break;
    }
    return programController == null ? null : programController.startListen();
  }

  private ProgramType getType(String typeName) {
    try {
      return ProgramType.valueOf(typeName.toUpperCase());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  // TODO (terence) : This method is part of the hack mentioned above. It should be removed when
  // FlowProgramRunner moved to run in AM.
  private Multimap<String, QueueName> getFlowletQueues(Program program, FlowSpecification flowSpec) {
    // Generate all queues specifications
    Id.Application appId = Id.Application.from(program.getNamespaceId(), program.getApplicationId());
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
  public ProgramLiveInfo getLiveInfo(Id.Program program, ProgramType type) {
    String twillAppName = String.format("%s.%s.%s.%s", type.name().toLowerCase(),
                                      program.getNamespaceId(), program.getApplicationId(), program.getId());
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
        DistributedProgramLiveInfo liveInfo = new DistributedProgramLiveInfo(program, type, report.getApplicationId());

        // if program type is flow then the container type is flowlet.
        Containers.ContainerType containerType = ProgramType.FLOW.equals(type) ? FLOWLET :
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

        // Add a list of announced services and their discoverables to the liveInfo.
        liveInfo.addServices(report.getServices());
        return liveInfo;
      }
    }
    return new NotRunningProgramLiveInfo(program, type);
  }

  /**
   * Reports resource usage of the cluster and all the app masters of running twill programs.
   */
  private class ClusterResourceReporter extends AbstractResourceReporter {
    private static final String RM_CLUSTER_METRICS_PATH = "/ws/v1/cluster/metrics";
    private final Path hbasePath;
    private final Path namedspacedPath;
    private final PathFilter namespacedFilter;
    private final List<URL> rmUrls;
    private final String namespace;
    private FileSystem hdfs;

    public ClusterResourceReporter(MetricsCollectionService metricsCollectionService, Configuration hConf,
                                   CConfiguration cConf) {
      super(metricsCollectionService.getCollector(
        ImmutableMap.<String, String>of()));
      try {
        this.hdfs = FileSystem.get(hConf);
      } catch (IOException e) {
        LOG.error("unable to get hdfs, cluster storage metrics will be unavailable");
        this.hdfs = null;
      }

      this.namespace = cConf.get(Constants.CFG_HDFS_NAMESPACE);
      this.namedspacedPath = new Path(namespace);
      this.hbasePath = new Path(hConf.get(HConstants.HBASE_DIR));
      this.namespacedFilter = new NamespacedPathFilter();
      List<URL> rmUrls = Collections.emptyList();
      try {
        rmUrls = getResourceManagerURLs(hConf);
      } catch (MalformedURLException e) {
        LOG.error("webapp address for the resourcemanager is malformed." +
                    " Cluster memory metrics will not be collected.", e);
      }
      LOG.trace("RM urls determined... {}", rmUrls);
      this.rmUrls = rmUrls;
    }

    // if ha resourcemanager is being used, need to read the config differently
    // HA rm has a setting for the rm ids, which is a comma separated list of ids.
    // it then has a separate webapp.address.<id> setting for each rm.
    private List<URL> getResourceManagerURLs(Configuration hConf) throws MalformedURLException {
      List<URL> urls = Lists.newArrayList();

      // if HA resource manager is enabled
      if (hConf.getBoolean(YarnConfiguration.RM_HA_ENABLED, false)) {
        LOG.trace("HA RM is enabled, determining webapp urls...");
        // for each resource manager
        for (String rmID : hConf.getStrings(YarnConfiguration.RM_HA_IDS)) {
          urls.add(getResourceURL(hConf, rmID));
        }
      } else {
        LOG.trace("HA RM is not enabled, determining webapp url...");
        urls.add(getResourceURL(hConf, null));
      }
      return urls;
    }

    // get the url for resource manager cluster metrics, given the id of the resource manager.
    private URL getResourceURL(Configuration hConf, String rmID) throws MalformedURLException {
      String setting = YarnConfiguration.RM_WEBAPP_ADDRESS;
      if (rmID != null) {
        setting += "." + rmID;
      }
      String addrStr = hConf.get(setting);
      // in HA mode, you can either set yarn.resourcemanager.hostname.<rm-id>,
      // or you can set yarn.resourcemanager.webapp.address.<rm-id>. In non-HA mode, the webapp address
      // is populated based on the resourcemanager hostname, but this is not the case in HA mode.
      // Therefore, if the webapp address is null, check for the resourcemanager hostname to derive the webapp address.
      if (addrStr == null) {
        // this setting is not a constant for some reason...
        setting = YarnConfiguration.RM_PREFIX + "hostname";
        if (rmID != null) {
          setting += "." + rmID;
        }
        addrStr = hConf.get(setting) + ":" + YarnConfiguration.DEFAULT_RM_WEBAPP_PORT;
      }
      addrStr = "http://" + addrStr + RM_CLUSTER_METRICS_PATH;
      LOG.trace("Adding {} as a rm address.", addrStr);
      return new URL(addrStr);
    }

    @Override
    public void reportResources() {
      for (TwillRunner.LiveInfo info : twillRunner.lookupLive()) {
        Map<String, String> metricContext = getMetricContext(info);
        if (metricContext == null) {
          continue;
        }

        // will have multiple controllers if there are multiple runs of the same application
        for (TwillController controller : info.getControllers()) {
          ResourceReport report = controller.getResourceReport();
          if (report == null) {
            continue;
          }
          int memory = report.getAppMasterResources().getMemoryMB();
          int vcores = report.getAppMasterResources().getVirtualCores();

          Map<String, String> runContext = ImmutableMap.<String, String>builder()
            .putAll(metricContext)
            .put(Constants.Metrics.Tag.RUN_ID, controller.getRunId().getId()).build();

          sendMetrics(runContext, 1, memory, vcores);
        }
      }
      reportClusterStorage();
      boolean reported = false;
      // if we have HA resourcemanager, need to cycle through possible webapps in case one is down.
      for (URL url : rmUrls) {
        // if we were able to hit the resource manager webapp, we don't have to try the others.
        if (reportClusterMemory(url)) {
          reported = true;
          break;
        }
      }
      if (!reported) {
        LOG.warn("unable to get resource manager metrics, cluster memory metrics will be unavailable");
      }
    }

    // YARN api is unstable, hit the webservice instead
    // returns whether or not it was able to hit the webservice.
    private boolean reportClusterMemory(URL url) {
      Reader reader = null;
      HttpURLConnection conn = null;
      LOG.trace("getting cluster memory from url {}", url);
      try {
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        reader = new InputStreamReader(conn.getInputStream(), Charsets.UTF_8);
        JsonObject response;
        try {
          response = new Gson().fromJson(reader, JsonObject.class);
        } catch (JsonParseException e) {
          // this is normal if this is not the active RM
          return false;
        }

        if (response != null) {
          JsonObject clusterMetrics = response.getAsJsonObject("clusterMetrics");
          long totalMemory = clusterMetrics.get("totalMB").getAsLong();
          long availableMemory = clusterMetrics.get("availableMB").getAsLong();
          MetricsCollector collector = getCollector();
          LOG.trace("resource manager, total memory = " + totalMemory + " available = " + availableMemory);
          collector.gauge("resources.total.memory", totalMemory);
          collector.gauge("resources.available.memory", availableMemory);
          return true;
        }
        return false;
      } catch (Exception e) {
        LOG.error("Exception getting cluster memory from ", e);
        return false;
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
        ContentSummary summary = hdfs.getContentSummary(namedspacedPath);
        long totalUsed = summary.getSpaceConsumed();
        long totalFiles = summary.getFileCount();
        long totalDirectories = summary.getDirectoryCount();

        // cdap hbase tables
        for (FileStatus fileStatus : hdfs.listStatus(hbasePath, namespacedFilter)) {
          summary = hdfs.getContentSummary(fileStatus.getPath());
          totalUsed += summary.getSpaceConsumed();
          totalFiles += summary.getFileCount();
          totalDirectories += summary.getDirectoryCount();
        }

        FsStatus hdfsStatus = hdfs.getStatus();
        long storageCapacity = hdfsStatus.getCapacity();
        long storageAvailable = hdfsStatus.getRemaining();

        MetricsCollector collector = getCollector();
        LOG.trace("total cluster storage = " + storageCapacity + " total used = " + totalUsed);
        collector.gauge("resources.total.storage", (storageCapacity / 1024 / 1024));
        collector.gauge("resources.available.storage", (storageAvailable / 1024 / 1024));
        collector.gauge("resources.used.storage", (totalUsed / 1024 / 1024));
        collector.gauge("resources.used.files", totalFiles);
        collector.gauge("resources.used.directories", totalDirectories);
      } catch (IOException e) {
        LOG.warn("Exception getting hdfs metrics", e);
      }
    }

    private class NamespacedPathFilter implements PathFilter {
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith(namespace);
      }
    }

    private Map<String, String> getMetricContext(TwillRunner.LiveInfo info) {
      Matcher matcher = APP_NAME_PATTERN.matcher(info.getApplicationName());
      if (!matcher.matches()) {
        return null;
      }

      ProgramType type = getType(matcher.group(1));
      if (type == null) {
        return null;
      }

      Id.Program programId = Id.Program.from(matcher.group(2), matcher.group(3), type, matcher.group(4));
      return getMetricsContext(type, programId);
    }
  }

  private static Map<String, String> getMetricsContext(ProgramType type, Id.Program programId) {
    return ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, programId.getNamespaceId(),
                           Constants.Metrics.Tag.APP, programId.getApplicationId(),
                           ProgramTypeMetricTag.getTagName(type), programId.getId());
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
