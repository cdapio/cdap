/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.preview;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.app.preview.PreviewManager;
import co.cask.cdap.app.preview.PreviewStatus;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.internal.app.deploy.ProgramTerminator;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.services.ApplicationLifecycleService;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.PreviewId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.common.Threads;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

/**
 * Default implementation of the {@link PreviewManager}.
 */
public class DefaultPreviewManager implements PreviewManager {
  private static final Gson GSON = new Gson();
  private final ApplicationLifecycleService applicationLifecycleService;
  private final ProgramLifecycleService programLifecycleService;
  private final Map<PreviewId, PreviewStatus> previewStatusMap = new ConcurrentHashMap<>();

  @Inject
  DefaultPreviewManager(ApplicationLifecycleService applicationLifecycleService,
                        ProgramLifecycleService programLifecycleService) {
    this.applicationLifecycleService = applicationLifecycleService;
    this.programLifecycleService = programLifecycleService;
  }

  @Override
  public PreviewId start(NamespaceId namespaceId, String config) throws Exception {
    final PreviewId previewId = new PreviewId(namespaceId.getNamespace(), RunIds.generate().getId());
    AppRequest<?> appRequest = GSON.fromJson(config, AppRequest.class);
    ArtifactSummary artifactSummary = appRequest.getArtifact();
    NamespaceId artifactNamespace =
      ArtifactScope.SYSTEM.equals(artifactSummary.getScope()) ? NamespaceId.SYSTEM : namespaceId;
    ArtifactId artifactId =
      new ArtifactId(artifactNamespace.getNamespace(), artifactSummary.getName(), artifactSummary.getVersion());

    // if we don't null check, it gets serialized to "null"
    String configString = appRequest.getConfig() == null ? null : GSON.toJson(appRequest.getConfig());
    try {
      applicationLifecycleService.deployApp(namespaceId.toId(), previewId.getPreview(), artifactId.toId(), configString,
                                            new ProgramTerminator() {
                                              @Override
                                              public void stop(Id.Program programId) throws Exception {

                                              }
                                            });
    } catch (Exception e) {
      previewStatusMap.put(previewId, new PreviewStatus(PreviewStatus.Status.DEPLOY_FAILED, e));
      throw Throwables.propagate(e);
    }

    // Assume that we are starting the SmartWorkflow in the DatapipelineApp
    ProgramId programId = new ProgramId(namespaceId.getNamespace(), previewId.getPreview(), ProgramType.WORKFLOW,
                                        "DataPipelineWorkflow");
    ProgramRuntimeService.RuntimeInfo runtimeInfo = programLifecycleService.start(programId,
                                                                                  new HashMap<String, String>(),
                                                                                  new HashMap<String, String>(), false);

    runtimeInfo.getController().addListener(new AbstractListener() {
      @Override
      public void init(ProgramController.State currentState, @Nullable Throwable cause) {
        previewStatusMap.put(previewId, new PreviewStatus(PreviewStatus.Status.RUNNING, null));
      }

      @Override
      public void completed() {
        previewStatusMap.put(previewId, new PreviewStatus(PreviewStatus.Status.COMPLETED, null));
      }

      @Override
      public void error(Throwable cause) {
        previewStatusMap.put(previewId, new PreviewStatus(PreviewStatus.Status.RUN_FAILED, cause));
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    return previewId;
  }

  @Override
  public PreviewStatus getStatus(PreviewId previewId) throws NotFoundException {
    if (!previewStatusMap.containsKey(previewId)) {
      throw new NotFoundException(previewId + " not found");
    }
    return previewStatusMap.get(previewId);
  }

  @Override
  public void stop(PreviewId previewId) throws Exception {
    if (!previewStatusMap.containsKey(previewId)) {
      throw new NotFoundException(previewId + " not found");
    }
    ProgramId programId = new ProgramId(previewId.getNamespace(), previewId.getPreview(), ProgramType.WORKFLOW,
                                        "DataPipelineWorkflow");
    programLifecycleService.stop(programId);
  }

  @Override
  public Map<String, Map<String, List<Object>>> getData(PreviewId previewId) throws NotFoundException {
    return new HashMap<>();
  }

  @Override
  public Collection<MetricTimeSeries> getMetrics(PreviewId previewId) throws NotFoundException {
    return new ArrayList<>();
  }

  @Override
  public List<LogEntry> getLogs(PreviewId previewId) throws NotFoundException {
    return new ArrayList<>();
  }
}
