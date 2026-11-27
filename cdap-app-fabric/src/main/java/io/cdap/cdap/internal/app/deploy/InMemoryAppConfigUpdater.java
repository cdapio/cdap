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

package io.cdap.cdap.internal.app.deploy;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.app.ApplicationUpdateResult;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.common.lang.CombineClassLoader;
import io.cdap.cdap.internal.app.DefaultApplicationUpdateContext;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.Artifacts;
import io.cdap.cdap.internal.app.worker.AppConfigUpdateRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import java.lang.reflect.Type;

/**
 * Executes {@link Application#updateConfig} in memory using the application's artifact classloader.
 */
public final class InMemoryAppConfigUpdater {

  private InMemoryAppConfigUpdater() {
  }

  /**
   * Updates application config in memory.
   *
   * @param request the app config update request
   * @param artifactClassLoader classloader to load application class from
   * @param artifactRepository artifact repository
   * @return JSON string of the updated configuration
   * @throws InvalidArtifactException if the application class cannot be loaded or instantiated, or
   *     a required dependency is missing
   * @throws UnsupportedOperationException if the application does not support update
   * @throws IllegalStateException if the application class is invalid or returns a null result
   * @throws Exception if update fails
   */
  public static String updateAppConfig(AppConfigUpdateRequest request,
      ClassLoader artifactClassLoader,
      ArtifactRepository artifactRepository) throws Exception {
    ApplicationId appId = request.getAppId();
    ArtifactId artifactId = request.getArtifactId();
    ApplicationSpecification appSpec = request.getAppSpec();
    DefaultApplicationUpdateContext updateContext = new DefaultApplicationUpdateContext(
        appId.getParent(), appId, artifactId.toApiArtifactId(), artifactRepository,
        appSpec.getConfiguration() == null ? "" : appSpec.getConfiguration(),
        request.getUpdateActions(), request.getAllowedArtifactScopes(),
        request.isAllowSnapshot(), appSpec);

    CombineClassLoader combineClassLoader = new CombineClassLoader(
        null, artifactClassLoader, InMemoryAppConfigUpdater.class.getClassLoader());
    ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(combineClassLoader);
    try {
      Application<?> app = instantiateApplication(
          appId, request.getApplicationClass(), artifactClassLoader);
      return executeConfigUpdate(appId, app, updateContext);
    } catch (Throwable t) {
      Artifacts.throwIfMissingDependency(t, artifactClassLoader);
      if (t instanceof Error) {
        throw (Error) t;
      }
      throw (Exception) t;
    } finally {
      ClassLoaders.setContextClassLoader(oldClassLoader);
    }
  }

  private static Application<?> instantiateApplication(ApplicationId appId,
      ApplicationClass appClass, ClassLoader artifactClassLoader) throws InvalidArtifactException {
    try {
      return Artifacts.instantiateApplication(artifactClassLoader, appClass.getClassName());
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      Artifacts.throwIfMissingDependency(e, artifactClassLoader);
      throw new InvalidArtifactException(
          String.format(
              "Failed to load or instantiate application class '%s' for application %s: %s",
              appClass.getClassName(), appId, detailMessage(e)), e);
    }
  }

  private static String executeConfigUpdate(ApplicationId appId, Application<?> app,
      DefaultApplicationUpdateContext updateContext) throws Exception {
    Type configType = Artifacts.getConfigType(app.getClass());
    if (!app.isUpdateSupported()) {
      throw new UnsupportedOperationException(
          String.format("Application %s does not support update.", appId));
    }

    ApplicationUpdateResult<?> updateResult = app.updateConfig(updateContext);
    if (updateResult == null) {
      throw new IllegalStateException(
          String.format("Application %s returned a null update result.", appId));
    }

    Gson gson = new GsonBuilder()
        .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
        .create();
    return Config.class.equals(configType)
        ? gson.toJson(updateResult.getNewConfig())
        : gson.toJson(updateResult.getNewConfig(), configType);
  }

  private static String detailMessage(Throwable t) {
    return t.getMessage() != null ? t.getMessage() : t.getClass().getName();
  }
}
