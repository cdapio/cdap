/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.deploy.pipeline;

import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ListenableFuture;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.app.deploy.ConfigResponse;
import io.cdap.cdap.app.deploy.Configurator;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.deploy.ConfiguratorFactory;
import io.cdap.cdap.internal.app.store.ApplicationMeta;
import io.cdap.cdap.internal.capability.CapabilityReader;
import io.cdap.cdap.metadata.MetadataValidator;
import io.cdap.cdap.pipeline.AbstractStage;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.twill.filesystem.Location;

/**
 * LocalArtifactLoaderStage gets a {@link Location} and emits a {@link ApplicationDeployable}.
 * <p>
 * This stage is responsible for reading the JAR and generating an ApplicationSpecification that is
 * forwarded to the next stage of processing.
 * </p>
 */
public class LocalArtifactLoaderStage extends AbstractStage<AppDeploymentInfo> {

  private final Store store;
  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;
  private final CapabilityReader capabilityReader;
  private final MetadataValidator metadataValidator;
  private final ConfiguratorFactory configuratorFactory;

  /**
   * Constructor with hit for handling type.
   */
  public LocalArtifactLoaderStage(CConfiguration cConf, Store store,
      AccessEnforcer accessEnforcer,
      AuthenticationContext authenticationContext,
      CapabilityReader capabilityReader,
      ConfiguratorFactory configuratorFactory) {
    super(TypeToken.of(AppDeploymentInfo.class));
    this.store = store;
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;
    this.capabilityReader = capabilityReader;
    this.metadataValidator = new MetadataValidator(cConf);
    this.configuratorFactory = configuratorFactory;
  }

  /**
   * Instantiates the Application class and calls configure() on it to generate the {@link
   * ApplicationSpecification}.
   *
   * @param deploymentInfo information needed to deploy the application, such as the artifact to
   *     create it from and the application config to use.
   */
  @Override
  public void process(AppDeploymentInfo deploymentInfo) throws Exception {

    String appVersion = deploymentInfo.getApplicationVersion();
    Configurator configurator = this.configuratorFactory.create(deploymentInfo);

    ListenableFuture<ConfigResponse> result = configurator.config();
    ConfigResponse response = result.get(120, TimeUnit.SECONDS);
    if (response.getExitCode() != 0) {
      throw new IllegalArgumentException("Failed to configure application: " + deploymentInfo);
    }
    AppSpecInfo appSpecInfo = response.getAppSpecInfo();
    if (appSpecInfo == null) {
      throw new IllegalArgumentException("Failed to configure application: " + deploymentInfo);
    }

    ApplicationSpecification specification = appSpecInfo.getAppSpec();
    ApplicationId applicationId;
    if (appVersion == null) {
      applicationId = deploymentInfo.getNamespaceId().app(specification.getName());
    } else {
      applicationId = deploymentInfo.getNamespaceId().app(specification.getName(), appVersion);
    }
    accessEnforcer.enforce(applicationId, authenticationContext.getPrincipal(),
        StandardPermission.CREATE);
    capabilityReader.checkAllEnabled(specification);

    Map<MetadataScope, Metadata> metadatas = appSpecInfo.getMetadata();
    for (Map.Entry<MetadataScope, Metadata> metadataEntry : metadatas.entrySet()) {
      Metadata metadata = metadataEntry.getValue();
      if (!metadata.getTags().isEmpty() || !metadata.getProperties().isEmpty()) {
        MetadataEntity appEntity = applicationId.toMetadataEntity();
        metadataValidator.validateProperties(appEntity, metadata.getProperties());
        metadataValidator.validateTags(appEntity, metadata.getTags());
      }
    }
    ApplicationSpecification appSpec = Optional.ofNullable(
            store.getLatest(applicationId.getAppReference()))
        .map(ApplicationMeta::getSpec)
        .orElse(null);
    emit(new ApplicationDeployable(deploymentInfo.getArtifactId(),
        deploymentInfo.getArtifactLocation(),
        applicationId, specification, appSpec,
        ApplicationDeployScope.USER, deploymentInfo.getApplicationClass(),
        deploymentInfo.getOwnerPrincipal(), deploymentInfo.canUpdateSchedules(),
        appSpecInfo.getSystemTables(), metadatas, deploymentInfo.getChangeDetail(),
        deploymentInfo.getSourceControlMeta(), deploymentInfo.isUpgrade(),
        deploymentInfo.isSkipMarkingLatest()));
  }
}
