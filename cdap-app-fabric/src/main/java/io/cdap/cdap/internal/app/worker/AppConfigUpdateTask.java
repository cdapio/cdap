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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.internal.remote.RunnableTaskModule;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.deploy.InMemoryAppConfigUpdater;
import io.cdap.cdap.internal.app.runtime.artifact.ApplicationClassCodec;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.RequirementsCodec;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerClient;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
      .registerTypeAdapterFactory(new AppConfigUpdateRequestTypeAdapterFactory())
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

  @VisibleForTesting
  Injector createInjector() {
    return Guice.createInjector(
        new ConfigModule(cConf),
        RemoteAuthenticatorModules.getDefaultModule(),
        new LocalLocationModule(),
        new ConfiguratorTaskModule(),
        new AuthenticationContextModules().getMasterWorkerModule(),
        new RunnableTaskModule(discoveryService, discoveryServiceClient, metricsCollectionService));
  }

  /**
   * Executes application config update in a task worker.
   *
   * @param context task execution context carrying the serialized {@link AppConfigUpdateRequest}
   * @throws IllegalArgumentException if the task parameter or deserialized request is invalid
   * @throws InvalidArtifactException if the application class cannot be loaded or instantiated, or
   *     a required dependency is missing
   * @throws UnsupportedOperationException if the application does not support config updates
   * @throws IllegalStateException if the application class is invalid or returns a null result
   * @throws Exception if artifact localization or {@code Application.updateConfig} fails
   */
  @Override
  public void run(RunnableTaskContext context) throws Exception {
    context.setTerminateOnComplete(true);

    String param = context.getParam();
    if (Strings.isNullOrEmpty(param)) {
      throw new IllegalArgumentException(
          "Invalid AppConfigUpdateRequest: task parameter must not be null or empty.");
    }
    AppConfigUpdateRequest request = GSON.fromJson(param, AppConfigUpdateRequest.class);

    Injector injector = createInjector();
    String updatedConfig = injector.getInstance(AppConfigUpdateTaskRunner.class).update(request);
    context.writeResult(updatedConfig.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Class to perform the app config update task execution.
   */
  @VisibleForTesting
  static class AppConfigUpdateTaskRunner {

    private final Impersonator impersonator;
    private final ArtifactRepository artifactRepository;
    private final ArtifactLocalizerClient artifactLocalizerClient;

    @Inject
    AppConfigUpdateTaskRunner(Impersonator impersonator,
        ArtifactRepository artifactRepository,
        ArtifactLocalizerClient artifactLocalizerClient) {
      this.impersonator = impersonator;
      this.artifactRepository = artifactRepository;
      this.artifactLocalizerClient = artifactLocalizerClient;
    }

    String update(AppConfigUpdateRequest request) throws Exception {
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
        return InMemoryAppConfigUpdater.updateAppConfig(
            request, artifactClassLoader, artifactRepository);
      }
    }
  }

  private static final class AppConfigUpdateRequestTypeAdapterFactory
      implements TypeAdapterFactory {

    @Override
    public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
      if (!AppConfigUpdateRequest.class.isAssignableFrom(type.getRawType())) {
        return null;
      }
      TypeAdapter<T> delegate = gson.getDelegateAdapter(this, type);
      return new TypeAdapter<T>() {
        @Override
        public void write(JsonWriter out, T value) throws IOException {
          delegate.write(out, value);
        }

        @Override
        public T read(JsonReader in) throws IOException {
          T value = delegate.read(in);
          if (value != null) {
            AppConfigUpdateRequest request = (AppConfigUpdateRequest) value;
            Preconditions.checkArgument(request.getAppId() != null,
                "Invalid AppConfigUpdateRequest: appId must not be null.");
            Preconditions.checkArgument(request.getArtifactId() != null,
                "Invalid AppConfigUpdateRequest: artifactId must not be null.");
            Preconditions.checkArgument(request.getApplicationClass() != null,
                "Invalid AppConfigUpdateRequest: applicationClass must not be null.");
            Preconditions.checkArgument(request.getAppSpec() != null,
                "Invalid AppConfigUpdateRequest: appSpec must not be null.");
            Preconditions.checkArgument(request.getUpdateActions() != null,
                "Invalid AppConfigUpdateRequest: updateActions must not be null.");
            Preconditions.checkArgument(request.getAllowedArtifactScopes() != null,
                "Invalid AppConfigUpdateRequest: allowedArtifactScopes must not be null.");
          }
          return value;
        }
      };
    }
  }
}
