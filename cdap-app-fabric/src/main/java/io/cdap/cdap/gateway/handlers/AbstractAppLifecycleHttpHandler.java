/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers;

import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.AbstractBodyConsumer;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.internal.app.deploy.ProgramTerminator;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.BodyConsumer;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Optional;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The abstract base class for the {@link AppLifecycleHttpHandler} and {@link AppLifecycleHttpHandlerInternal}
 * It contains the common methods used in both handlers mentioned above.
 */
public abstract class AbstractAppLifecycleHttpHandler extends AbstractAppFabricHttpHandler {
  protected static final Gson GSON = new Gson();
  protected static final Gson DECODE_GSON = new GsonBuilder()
      .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
      .create();
  protected static final Logger LOG = LoggerFactory.getLogger(AbstractAppLifecycleHttpHandler.class);

  protected final CConfiguration configuration;
  protected final NamespaceQueryAdmin namespaceQueryAdmin;
  protected final ProgramRuntimeService runtimeService;
  protected final ApplicationLifecycleService applicationLifecycleService;
  protected final File tmpDir;

  /**
   * Constructor for the abstract class.
   *
   * @param configuration CConfiguration, passed from the derived class where it's injected
   * @param namespaceQueryAdmin passed from the derived class where it's injected
   * @param runtimeService passed from the derived class where it's injected
   */
  public AbstractAppLifecycleHttpHandler(CConfiguration configuration,
      NamespaceQueryAdmin namespaceQueryAdmin,
      ProgramRuntimeService runtimeService,
      ApplicationLifecycleService applicationLifecycleService) {
    this.configuration = configuration;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.runtimeService = runtimeService;
    this.applicationLifecycleService = applicationLifecycleService;
    this.tmpDir = new File(new File(configuration.get(Constants.CFG_LOCAL_DATA_DIR)),
        configuration.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
  }

  protected ApplicationId validateApplicationVersionId(NamespaceId namespaceId, String appId,
      String versionId) throws BadRequestException {
    if (appId == null) {
      throw new BadRequestException("Path parameter app-id cannot be empty");
    }
    if (!EntityId.isValidId(appId)) {
      throw new BadRequestException(String.format("Invalid app name '%s'", appId));
    }
    if (versionId == null) {
      throw new BadRequestException("Path parameter version-id cannot be empty");
    }
    if (EntityId.isValidVersionId(versionId)) {
      return namespaceId.app(appId, versionId);
    }
    throw new BadRequestException(String.format("Invalid version '%s'", versionId));
  }

  protected NamespaceId validateNamespace(@Nullable String namespace)
      throws BadRequestException, NamespaceNotFoundException, AccessException {

    if (namespace == null) {
      throw new BadRequestException("Path parameter namespace-id cannot be empty");
    }

    NamespaceId namespaceId;
    try {
      namespaceId = new NamespaceId(namespace);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(String.format("Invalid namespace '%s'", namespace), e);
    }

    try {
      if (!namespaceId.equals(NamespaceId.SYSTEM)) {
        namespaceQueryAdmin.get(namespaceId);
      }
    } catch (NamespaceNotFoundException | AccessException e) {
      throw e;
    } catch (Exception e) {
      // This can only happen when NamespaceAdmin uses HTTP calls to interact with namespaces.
      // In AppFabricServer, NamespaceAdmin is bound to DefaultNamespaceAdmin, which interacts directly with the MDS.
      // Hence, this exception will never be thrown
      throw Throwables.propagate(e);
    }
    return namespaceId;
  }

  protected ProgramTerminator createProgramTerminator() {
    return programId -> {
      switch (programId.getType()) {
        case SERVICE:
        case WORKER:
          killProgramIfRunning(programId);
          break;
        default:
          break;
      }
    };
  }

  protected void killProgramIfRunning(ProgramId programId) {
    ProgramRuntimeService.RuntimeInfo programRunInfo = findRuntimeInfo(programId, runtimeService);
    if (programRunInfo != null) {
      ProgramController controller = programRunInfo.getController();
      controller.kill();
    }
  }

  protected ApplicationRecord getApplicationRecord(ApplicationWithPrograms deployedApp) {
    return new ApplicationRecord(
        ArtifactSummary.from(deployedApp.getArtifactId().toApiArtifactId()),
        deployedApp.getApplicationId().getApplication(),
        deployedApp.getApplicationId().getVersion(),
        deployedApp.getSpecification().getDescription(),
        Optional.ofNullable(deployedApp.getOwnerPrincipal()).map(KerberosPrincipalId::getPrincipal)
            .orElse(null),
        deployedApp.getChangeDetail(), null);
  }

  protected BodyConsumer deployAppFromArtifact(
      final ApplicationId appId,
      final boolean skipMarkingLatest)
      throws IOException {
    return new AbstractBodyConsumer(
        File.createTempFile("apprequest-" + appId, ".json", tmpDir)) {
      @Override
      protected void onFinish(HttpResponder responder, File uploadedFile) {
        try (FileReader fileReader = new FileReader(uploadedFile)) {
          AppRequest<?> appRequest = DECODE_GSON.fromJson(fileReader, AppRequest.class);

          try {
            ApplicationWithPrograms app = applicationLifecycleService.deployApp(appId, appRequest,
                null, createProgramTerminator(), skipMarkingLatest);
            responder.sendJson(HttpResponseStatus.OK, GSON.toJson(getApplicationRecord(app)));
          } catch (DatasetManagementException e) {
            if (e.getCause() instanceof UnauthorizedException) {
              throw (UnauthorizedException) e.getCause();
            } else {
              throw e;
            }
          }
        } catch (ArtifactNotFoundException e) {
          responder.sendString(HttpResponseStatus.NOT_FOUND, e.getMessage());
        } catch (ConflictException e) {
          responder.sendString(HttpResponseStatus.CONFLICT, e.getMessage());
        } catch (UnauthorizedException e) {
          responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
        } catch (InvalidArtifactException e) {
          responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        } catch (IOException e) {
          LOG.error("Error reading request body for creating app {}.", appId, e);
          responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, String.format(
              "Error while reading json request body for app %s.", appId));
        } catch (Exception e) {
          LOG.error("Deploy failure", e);
          responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        }
      }
    };
  }
}
