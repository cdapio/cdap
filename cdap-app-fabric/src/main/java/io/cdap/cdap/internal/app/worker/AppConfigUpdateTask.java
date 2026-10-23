/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.app.ApplicationConfigUpdateAction;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.app.ApplicationUpdateResult;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.common.lang.CombineClassLoader;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.DefaultApplicationUpdateContext;
import io.cdap.cdap.internal.app.runtime.artifact.ApplicationClassCodec;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.Artifacts;
import io.cdap.cdap.internal.app.runtime.artifact.RequirementsCodec;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerClient;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;

/**
 * RunnableTask for updating application config on a TaskWorker.
 */
public class AppConfigUpdateTask implements RunnableTask {

  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(
      new GsonBuilder())
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .registerTypeAdapter(ApplicationClass.class, new ApplicationClassCodec())
      .registerTypeAdapter(Requirements.class, new RequirementsCodec())
      .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
      .create();

  private final CConfiguration cConf;
  private final DiscoveryService discoveryService;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final MetricsCollectionService metricsCollectionService;

  @Inject
  AppConfigUpdateTask(CConfiguration cConf,
      DiscoveryService discoveryService,
      DiscoveryServiceClient discoveryServiceClient,
      MetricsCollectionService metricsCollectionService) {
    this.cConf = cConf;
    this.discoveryService = discoveryService;
    this.discoveryServiceClient = discoveryServiceClient;
    this.metricsCollectionService = metricsCollectionService;
  }

  @Override
  public void run(RunnableTaskContext context) throws Exception {
    context.setTerminateOnComplete(true);

    AppConfigUpdateRequest request = GSON.fromJson(context.getParam(), AppConfigUpdateRequest.class);
    validateRequest(request);

    Injector injector = ConfiguratorTask.createInjector(cConf, discoveryService,
        discoveryServiceClient, metricsCollectionService);
    String updatedConfig = update(
        request,
        injector.getInstance(Impersonator.class),
        injector.getInstance(ArtifactRepository.class),
        injector.getInstance(ArtifactLocalizerClient.class));

    context.writeResult(updatedConfig.getBytes(StandardCharsets.UTF_8));
  }

  static String update(AppConfigUpdateRequest request,
      Impersonator impersonator,
      ArtifactRepository artifactRepository,
      ArtifactLocalizerClient artifactLocalizerClient) throws Exception {
    validateRequest(request);

    Location artifactLocation = Locations.toLocation(
        artifactLocalizerClient.getUnpackedArtifactLocation(request.getArtifactId()));

    ArtifactDescriptor descriptor = new ArtifactDescriptor(
        request.getArtifactId().getNamespace(),
        request.getArtifactId().toApiArtifactId(),
        artifactLocation);

    EntityImpersonator entityImpersonator =
        new EntityImpersonator(request.getArtifactId(), impersonator);

    try (CloseableClassLoader artifactClassLoader = artifactRepository.createArtifactClassLoader(
        descriptor, entityImpersonator)) {
      return updateAppConfig(
          request.getAppId(),
          request.getArtifactId(),
          request.getApplicationClass(),
          request.getAppSpec(),
          artifactClassLoader,
          artifactRepository,
          request.getUpdateActions(),
          request.getAllowedArtifactScopes(),
          request.isAllowSnapshot());
    }
  }

  /**
   * Updates application config in memory.
   *
   * @param appId the id of the application to update
   * @param artifactId the artifact id of the application
   * @param appClass the application class metadata
   * @param appSpec current application specification
   * @param artifactClassLoader classloader to load application class from
   * @param artifactRepository artifact repository
   * @param updateActions list of update actions
   * @param allowedArtifactScopes allowed artifact scopes
   * @param allowSnapshot whether snapshot versions are allowed
   * @return JSON string of the updated configuration
   * @throws Exception if update fails
   */
  public static String updateAppConfig(ApplicationId appId,
      ArtifactId artifactId,
      ApplicationClass appClass,
      ApplicationSpecification appSpec,
      ClassLoader artifactClassLoader,
      ArtifactRepository artifactRepository,
      List<ApplicationConfigUpdateAction> updateActions,
      Set<ArtifactScope> allowedArtifactScopes,
      boolean allowSnapshot) throws Exception {
    Class<? extends Application> applicationClass =
        Artifacts.loadApplicationClass(artifactClassLoader, appClass.getClassName());
    Type configType = Artifacts.getConfigType(applicationClass);

    // Scoped locally to avoid leaking Config classes via Gson's TypeAdapter cache,
    // which would prevent CloseableClassLoader from being garbage-collected.
    Gson gson = new GsonBuilder()
        .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
        .create();

    DefaultApplicationUpdateContext updateContext = new DefaultApplicationUpdateContext(
        appId.getParent(), appId, artifactId.toApiArtifactId(), artifactRepository,
        appSpec.getConfiguration() == null ? "" : appSpec.getConfiguration(),
        updateActions, allowedArtifactScopes, allowSnapshot, appSpec);

    try (CombineClassLoader combineClassLoader = new CombineClassLoader(
        null, artifactClassLoader, AppConfigUpdateTask.class.getClassLoader())) {
      ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(combineClassLoader);
      try {
        Application<?> app = Artifacts.instantiateApplication(applicationClass);

        if (!app.isUpdateSupported()) {
          throw new UnsupportedOperationException(
              String.format("Application %s does not support update.", appId));
        }

        ApplicationUpdateResult<?> updateResult = app.updateConfig(updateContext);
        if (updateResult == null || updateResult.getNewConfig() == null) {
          throw new IllegalStateException(
              String.format(
                  "Application %s returned a null update result or configuration.", appId));
        }

        return (configType == null || Config.class.equals(configType))
            ? gson.toJson(updateResult.getNewConfig())
            : gson.toJson(updateResult.getNewConfig(), configType);
      } finally {
        ClassLoaders.setContextClassLoader(oldClassLoader);
      }
    }
  }

  private static void validateRequest(AppConfigUpdateRequest request) {
    if (request == null || request.getAppId() == null || request.getArtifactId() == null
        || request.getApplicationClass() == null || request.getAppSpec() == null
        || request.getUpdateActions() == null || request.getAllowedArtifactScopes() == null) {
      throw new IllegalArgumentException(
          "Invalid AppConfigUpdateRequest: missing required fields.");
    }
  }
}
