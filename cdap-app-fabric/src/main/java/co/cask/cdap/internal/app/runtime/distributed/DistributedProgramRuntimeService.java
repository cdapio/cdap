/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.AbstractProgramRuntimeService;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramResourceReporter;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.internal.app.program.ProgramTypeMetricTag;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.AbstractResourceReporter;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactDetail;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.service.SimpleRuntimeInfo;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.Containers;
import co.cask.cdap.proto.DistributedProgramLiveInfo;
import co.cask.cdap.proto.NotRunningProgramLiveInfo;
import co.cask.cdap.proto.ProgramLiveInfo;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.security.impersonation.Impersonator;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.internal.yarn.YarnUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

import static co.cask.cdap.proto.Containers.ContainerInfo;
import static co.cask.cdap.proto.Containers.ContainerType.FLOWLET;

/**
 *
 */
public final class DistributedProgramRuntimeService extends AbstractProgramRuntimeService {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedProgramRuntimeService.class);

  private final TwillRunner twillRunner;

  // TODO (terence): Injection of Store and QueueAdmin is a hack for queue reconfiguration.
  // Need to remove it when FlowProgramRunner can runs inside Twill AM.
  private final ProgramRunnerFactory programRunnerFactory;
  private final Store store;
  private final ProgramResourceReporter resourceReporter;
  private final Impersonator impersonator;
  private final ProgramStateWriter programStateWriter;

  @Inject
  DistributedProgramRuntimeService(ProgramRunnerFactory programRunnerFactory, TwillRunner twillRunner, Store store,
                                   MetricsCollectionService metricsCollectionService,
                                   Configuration hConf, CConfiguration cConf,
                                   // for running a program, we only need EXECUTE on the program, there should be no
                                   // privileges needed for artifacts
                                   @Named(AppFabricServiceRuntimeModule.NOAUTH_ARTIFACT_REPO)
                                     ArtifactRepository noAuthArtifactRepository,
                                   Impersonator impersonator, ProgramStateWriter programStateWriter) {
    super(cConf, programRunnerFactory, noAuthArtifactRepository, programStateWriter);
    this.programRunnerFactory = programRunnerFactory;
    this.twillRunner = twillRunner;
    this.store = store;
    this.resourceReporter = new ClusterResourceReporter(metricsCollectionService, hConf);
    this.impersonator = impersonator;
    this.programStateWriter = programStateWriter;
  }

  @Nullable
  @Override
  protected RuntimeInfo createRuntimeInfo(final ProgramController controller, final ProgramId programId,
                                          final Runnable cleanUpTask) {
    if (controller instanceof AbstractTwillProgramController) {
      RunId twillRunId = ((AbstractTwillProgramController) controller).getTwillRunId();

      // Add a listener that publishes KILLED status notification when the YARN application is killed in case that
      // the KILLED status notification is not published from the YARN application container, so we don't need to wait
      // for the run record corrector to mark the status as KILLED.
      controller.addListener(new AbstractListener() {
        @Override
        public void init(ProgramController.State currentState, @Nullable Throwable cause) {
          if (currentState == ProgramController.State.ALIVE) {
            alive();
          } else if (currentState == ProgramController.State.KILLED) {
            killed();
          }
        }

        @Override
        public void alive() {
          cleanUpTask.run();
        }

        @Override
        public void killed() {
          programStateWriter.killed(programId.run(controller.getRunId()));
        }
      }, MoreExecutors.sameThreadExecutor());
      return new SimpleRuntimeInfo(controller, programId, twillRunId);
    }
    return null;
  }

  @Override
  protected void copyArtifact(ArtifactId artifactId,
                              final ArtifactDetail artifactDetail, final File targetFile) throws IOException {
    try {
      impersonator.doAs(artifactId, new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          Locations.linkOrCopy(artifactDetail.getDescriptor().getLocation(), targetFile);
          return null;
        }
      });
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, IOException.class);
      // should not happen
      throw Throwables.propagate(e);
    }
  }

  // TODO SAGAR better data structure to support this efficiently
  private synchronized boolean isTwillRunIdCached(RunId twillRunId) {
    for (RuntimeInfo runtimeInfo : getRuntimeInfos()) {
      if (twillRunId.equals(runtimeInfo.getTwillRunId())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public synchronized RuntimeInfo lookup(ProgramId programId, final RunId runId) {
    RuntimeInfo runtimeInfo = super.lookup(programId, runId);
    if (runtimeInfo != null) {
      return runtimeInfo;
    }

    // Goes through all live application and fill the twillProgramInfo table
    for (TwillRunner.LiveInfo liveInfo : twillRunner.lookupLive()) {
      String appName = liveInfo.getApplicationName();
      ProgramId id = TwillAppNames.fromTwillAppName(appName, false);
      if (id == null) {
        continue;
      }

      if (!id.equals(programId)) {
        continue;
      }

      // Program matched
      RunRecordMeta record = store.getRun(programId, runId.getId());
      if (record == null) {
        return null;
      }
      if (record.getTwillRunId() == null) {
        LOG.warn("Twill RunId does not exist for the program {}, runId {}", programId, runId.getId());
        return null;
      }
      RunId twillRunIdFromRecord = org.apache.twill.internal.RunIds.fromString(record.getTwillRunId());

      for (TwillController controller : liveInfo.getControllers()) {
        RunId twillRunId = controller.getRunId();
        if (!twillRunId.equals(twillRunIdFromRecord)) {
          continue;
        }
        runtimeInfo = createRuntimeInfo(programId, controller, runId);
        if (runtimeInfo != null) {
          updateRuntimeInfo(programId.getType(), runId, runtimeInfo);
        } else {
          LOG.warn("Unable to find program for runId {}", runId);
        }
        return runtimeInfo;
      }
    }
    return null;
  }

  @Override
  public synchronized Map<RunId, RuntimeInfo> list(ProgramType type) {
    Map<RunId, RuntimeInfo> result = Maps.newHashMap();
    result.putAll(super.list(type));

    // Table holds the Twill RunId and TwillController associated with the program matching the input type
    Table<ProgramId, RunId, TwillController> twillProgramInfo = HashBasedTable.create();

    // Goes through all live application and fill the twillProgramInfo table
    for (TwillRunner.LiveInfo liveInfo : twillRunner.lookupLive()) {
      String appName = liveInfo.getApplicationName();
      ProgramId programId = TwillAppNames.fromTwillAppName(appName, false);
      if (programId == null) {
        continue;
      }
      if (!type.equals(programId.getType())) {
        continue;
      }

      for (TwillController controller : liveInfo.getControllers()) {
        RunId twillRunId = controller.getRunId();
        if (isTwillRunIdCached(twillRunId)) {
          continue;
        }

        twillProgramInfo.put(programId, twillRunId, controller);
      }
    }

    if (twillProgramInfo.isEmpty()) {
      return ImmutableMap.copyOf(result);
    }

    final Set<RunId> twillRunIds = twillProgramInfo.columnKeySet();
    Collection<RunRecordMeta> activeRunRecords = store.getRuns(ProgramRunStatus.RUNNING,
                                                               new Predicate<RunRecordMeta>() {
      @Override
      public boolean apply(RunRecordMeta record) {
        return record.getTwillRunId() != null
          && twillRunIds.contains(org.apache.twill.internal.RunIds.fromString(record.getTwillRunId()));
      }
    }).values();

    for (RunRecordMeta record : activeRunRecords) {
      String twillRunId = record.getTwillRunId();
      if (twillRunId == null) {
        // This is unexpected. Just log and ignore the run record
        LOG.warn("No twill runId for in run record {}.", record);
        continue;
      }

      RunId twillRunIdFromRecord = org.apache.twill.internal.RunIds.fromString(twillRunId);
      // Get the CDAP RunId from RunRecord
      RunId runId = RunIds.fromString(record.getPid());
      // Get the Program and TwillController for the current twillRunId
      Map<ProgramId, TwillController> mapForTwillId = twillProgramInfo.columnMap().get(twillRunIdFromRecord);
      Map.Entry<ProgramId, TwillController> entry = mapForTwillId.entrySet().iterator().next();

      // Create RuntimeInfo for the current Twill RunId
      RuntimeInfo runtimeInfo = createRuntimeInfo(entry.getKey(), entry.getValue(), runId);
      if (runtimeInfo != null) {
        result.put(runId, runtimeInfo);
        updateRuntimeInfo(type, runId, runtimeInfo);
      } else {
        LOG.warn("Unable to find program {} {}", type, entry.getKey());
      }
    }

    return ImmutableMap.copyOf(result);
  }

  @Nullable
  private RuntimeInfo createRuntimeInfo(ProgramId programId, TwillController controller, RunId runId) {
    try {
      ProgramDescriptor programDescriptor = store.loadProgram(programId);
      ProgramController programController = createController(programDescriptor, controller, runId);
      return programController == null ? null : new SimpleRuntimeInfo(programController,
                                                                      programId,
                                                                      controller.getRunId());
    } catch (Exception e) {
      return null;
    }
  }

  @Nullable
  private ProgramController createController(ProgramDescriptor programDescriptor,
                                             TwillController controller, RunId runId) {
    ProgramId programId = programDescriptor.getProgramId();
    ProgramRunner programRunner;
    try {
      programRunner = programRunnerFactory.create(programId.getType());
    } catch (IllegalArgumentException e) {
      // This shouldn't happen. If it happen, it means CDAP was incorrectly install such that some of the program
      // type is not support (maybe due to version mismatch in upgrade).
      LOG.error("Unsupported program type {} for program {}. " +
                  "It is likely caused by incorrect CDAP installation or upgrade to incompatible CDAP version",
                programId.getType(), programId);
      return null;
    }

    if (!(programRunner instanceof DistributedProgramRunner)) {
      // This is also unexpected. If it happen, it means the CDAP core or the runtime provider extension was wrongly
      // implemented
      ResourceReport resourceReport = controller.getResourceReport();
      LOG.error("Unable to create ProgramController for program {} for twill application {}. It is likely caused by " +
                  "invalid CDAP program runtime extension.",
                programId, resourceReport == null ? "'unknown twill application'" : resourceReport.getApplicationId());
      return null;
    }

    return ((DistributedProgramRunner) programRunner).createProgramController(controller, programDescriptor, runId);
  }

  @Override
  public ProgramLiveInfo getLiveInfo(ProgramId program) {
    String twillAppName = TwillAppNames.toTwillAppName(program);
    Iterator<TwillController> controllers = twillRunner.lookup(twillAppName).iterator();
    // this will return an empty Json if there is no live instance
    if (controllers.hasNext()) {
      TwillController controller = controllers.next();
      if (controllers.hasNext()) {
        LOG.warn("Expected at most one live instance of Twill app {} but found at least two.", twillAppName);
      }
      ResourceReport report = controller.getResourceReport();
      if (report != null) {
        DistributedProgramLiveInfo liveInfo = new DistributedProgramLiveInfo(program, report.getApplicationId());

        // if program type is flow then the container type is flowlet.
        Containers.ContainerType containerType = ProgramType.FLOW.equals(program.getType()) ? FLOWLET :
                                                 Containers.ContainerType.valueOf(program.getType().name());

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
    return new NotRunningProgramLiveInfo(program);
  }

  /**
   * Reports resource usage of the cluster and all the app masters of running twill programs.
   */
  private class ClusterResourceReporter extends AbstractResourceReporter {

    private final YarnClient yarnClient;

    ClusterResourceReporter(MetricsCollectionService metricsCollectionService, Configuration hConf) {
      super(metricsCollectionService.getContext(ImmutableMap.<String, String>of()));

      YarnClient yarnClient = YarnClient.createYarnClient();
      yarnClient.init(hConf);
      this.yarnClient = yarnClient;
    }

    @Override
    protected void startUp() throws Exception {
      super.startUp();
      yarnClient.start();
    }

    @Override
    protected void shutDown() throws Exception {
      yarnClient.stop();
      super.shutDown();
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
      reportYarnResources();
    }

    private void reportYarnResources() {
      try {
        long totalMemory = 0L;
        long totalVCores = 0L;
        long usedMemory = 0L;
        long usedVCores = 0L;

        for (NodeReport nodeReport : yarnClient.getNodeReports(NodeState.RUNNING)) {
          Resource capability = nodeReport.getCapability();
          Resource used = nodeReport.getUsed();

          totalMemory += capability.getMemory();
          totalVCores += YarnUtils.getVirtualCores(capability);

          usedMemory += used.getMemory();
          usedVCores += YarnUtils.getVirtualCores(used);
        }

        MetricsContext collector = getCollector();

        LOG.trace("YARN Cluster memory total={}MB, used={}MB", totalMemory, usedMemory);
        collector.gauge("resources.total.memory", totalMemory);
        collector.gauge("resources.used.memory", usedMemory);
        collector.gauge("resources.available.memory", totalMemory - usedMemory);

        LOG.trace("YARN Cluster vcores total={}, used={}", totalVCores, usedVCores);
        collector.gauge("resources.total.vcores", totalVCores);
        collector.gauge("resources.used.vcores", usedVCores);
        collector.gauge("resources.available.vcores", totalVCores - usedVCores);

      } catch (Exception e) {
        LOG.warn("Failed to gather YARN NodeReports", e);
      }
    }

    private Map<String, String> getMetricContext(TwillRunner.LiveInfo info) {
      ProgramId programId = TwillAppNames.fromTwillAppName(info.getApplicationName(), false);
      if (programId == null) {
        return null;
      }

      return getMetricsContext(programId.getType(), programId);
    }
  }

  private static Map<String, String> getMetricsContext(ProgramType type, ProgramId programId) {
    return ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, programId.getNamespace(),
                           Constants.Metrics.Tag.APP, programId.getApplication(),
                           ProgramTypeMetricTag.getTagName(type), programId.getProgram());
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
