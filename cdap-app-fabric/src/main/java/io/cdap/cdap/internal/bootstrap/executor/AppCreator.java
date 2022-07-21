/*
 * Copyright © 2018-2019 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.internal.bootstrap.executor;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

import java.util.Collections;

/**
 * Creates an application if it doesn't already exist.
 */
public class AppCreator extends BaseStepExecutor<AppCreator.Arguments> {
  private static final Gson GSON = new Gson();
  private final ApplicationLifecycleService appLifecycleService;

  @Inject
  AppCreator(ApplicationLifecycleService appLifecycleService) {
    this.appLifecycleService = appLifecycleService;
  }

  @Override
  public void execute(Arguments arguments) throws Exception {
    ApplicationId appId = arguments.getId();
    ArtifactSummary artifactSummary = arguments.getArtifact();

    if (appExists(appId) && !arguments.overwrite) {
      return;
    }

    KerberosPrincipalId ownerPrincipalId =
      arguments.getOwnerPrincipal() == null ? null : new KerberosPrincipalId(arguments.getOwnerPrincipal());

    // if we don't null check, it gets serialized to "null"
    String configString = arguments.getConfig() == null ? null : GSON.toJson(arguments.getConfig());

    try {
      appLifecycleService.deployApp(appId.getParent(), appId.getApplication(), appId.getVersion(),
                                    artifactSummary, configString, x -> { },
                                    ownerPrincipalId, arguments.canUpdateSchedules(), false, Collections.emptyMap());
    } catch (NotFoundException | UnauthorizedException | InvalidArtifactException e) {
      // these exceptions are for sure not retry-able. It's hard to tell if the others are, so we just try retrying
      // up to the default time limit
      throw e;
    } catch (DatasetManagementException e) {
      if (e.getCause() instanceof UnauthorizedException) {
        throw (UnauthorizedException) e.getCause();
      } else {
        throw new RetryableException(e);
      }
    } catch (Exception e) {
      throw new RetryableException(e);
    }
  }

  private boolean appExists(ApplicationId applicationId) throws Exception {
    try {
      appLifecycleService.getAppDetail(applicationId);
      return true;
    } catch (ApplicationNotFoundException e) {
      return false;
    }
  }

  /**
   * Arguments required to create an application
   */
  static class Arguments extends AppRequest<JsonObject> implements Validatable {
    private String namespace;
    private String name;
    private boolean overwrite;

    Arguments(AppRequest<JsonObject> appRequest, String namespace, String name, boolean overwrite) {
      super(appRequest.getArtifact(), appRequest.getConfig(), appRequest.getPreview(), appRequest.getOwnerPrincipal(),
            appRequest.canUpdateSchedules());
      this.namespace = namespace;
      this.name = name;
      this.overwrite = overwrite;
    }

    private ApplicationId getId() {
      return new NamespaceId(namespace).app(name);
    }

    @Override
    public void validate() {
      super.validate();
      if (namespace == null || namespace.isEmpty()) {
        throw new IllegalArgumentException("Namespace must be specified");
      }
      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Application name must be specified");
      }
      getId();
    }
  }
}
