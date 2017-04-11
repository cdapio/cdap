/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.app.deploy.ConfigResponse;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.deploy.InMemoryConfigurator;
import co.cask.cdap.internal.app.deploy.LocalApplicationManager;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.pipeline.Context;
import co.cask.cdap.pipeline.Pipeline;
import co.cask.cdap.pipeline.Stage;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.security.impersonation.EntityImpersonator;
import co.cask.cdap.security.impersonation.Impersonator;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * LocalArtifactLoaderStage gets a {@link Location} and emits a {@link ApplicationDeployable}.
 * <p>
 * This stage is responsible for reading the JAR and generating an ApplicationSpecification
 * that is forwarded to the next stage of processing.
 * </p>
 * An active {@link ClassLoader} for the artifact used during deployment will be set to the {@link Context} property
 * with the key {@link LocalApplicationManager#ARTIFACT_CLASSLOADER_KEY}.
 * It is expected a {@link Pipeline#setFinally(Stage)} stage to clean it up after the pipeline execution finished.
 */
public class LocalArtifactLoaderStage extends AbstractStage<AppDeploymentInfo> {
  private final CConfiguration cConf;
  private final Store store;
  private final ApplicationSpecificationAdapter adapter;
  private final ArtifactRepository artifactRepository;
  private final Impersonator impersonator;

  /**
   * Constructor with hit for handling type.
   */
  public LocalArtifactLoaderStage(CConfiguration cConf, Store store, ArtifactRepository artifactRepository,
                                  Impersonator impersonator) {
    super(TypeToken.of(AppDeploymentInfo.class));
    this.cConf = cConf;
    this.store = store;
    this.adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    this.artifactRepository = artifactRepository;
    this.impersonator = impersonator;
  }

  /**
   * Instantiates the Application class and calls configure() on it to generate the {@link ApplicationSpecification}.
   *
   * @param deploymentInfo information needed to deploy the application, such as the artifact to create it from
   *                       and the application config to use.
   */
  @Override
  public void process(AppDeploymentInfo deploymentInfo)
    throws InterruptedException, ExecutionException, TimeoutException, IOException {

    ArtifactId artifactId = deploymentInfo.getArtifactId();
    Location artifactLocation = deploymentInfo.getArtifactLocation();
    String appClassName = deploymentInfo.getAppClassName();
    String appVersion = deploymentInfo.getApplicationVersion();
    String configString = deploymentInfo.getConfigString();

    EntityImpersonator classLoaderImpersonator =
      new EntityImpersonator(artifactId, impersonator);
    ClassLoader artifactClassLoader = artifactRepository.createArtifactClassLoader(artifactLocation,
                                                                                   classLoaderImpersonator);
    getContext().setProperty(LocalApplicationManager.ARTIFACT_CLASSLOADER_KEY, artifactClassLoader);

    InMemoryConfigurator inMemoryConfigurator = new InMemoryConfigurator(cConf, deploymentInfo.getNamespaceId().toId(),
                                                                         artifactId.toId(), appClassName,
                                                                         artifactRepository, artifactClassLoader,
                                                                         deploymentInfo.getApplicationName(),
                                                                         deploymentInfo.getApplicationVersion(),
                                                                         configString);

    ListenableFuture<ConfigResponse> result = inMemoryConfigurator.config();
    ConfigResponse response = result.get(120, TimeUnit.SECONDS);
    if (response.getExitCode() != 0) {
      throw new IllegalArgumentException("Failed to configure application: " + deploymentInfo);
    }
    ApplicationSpecification specification = adapter.fromJson(response.get());
    ApplicationId applicationId;
    if (appVersion == null) {
      applicationId = deploymentInfo.getNamespaceId().app(specification.getName());
    } else {
      applicationId = deploymentInfo.getNamespaceId().app(specification.getName(), appVersion);
    }
    emit(new ApplicationDeployable(deploymentInfo.getArtifactId(), deploymentInfo.getArtifactLocation(),
                                   applicationId, specification, store.getApplication(applicationId),
                                   ApplicationDeployScope.USER, deploymentInfo.getOwnerPrincipal(),
                                   deploymentInfo.canUpdateSchedules()));
  }
}
