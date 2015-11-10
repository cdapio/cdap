/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.internal.app.ForwardingApplicationSpecification;
import co.cask.cdap.internal.app.deploy.InMemoryConfigurator;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.Id;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * LocalArtifactLoaderStage gets a {@link Location} and emits a {@link ApplicationDeployable}.
 * <p>
 * This stage is responsible for reading the JAR and generating an ApplicationSpecification
 * that is forwarded to the next stage of processing.
 * </p>
 */
public class LocalArtifactLoaderStage extends AbstractStage<AppDeploymentInfo> {
  private final CConfiguration cConf;
  private final Store store;
  private final Id.Namespace namespace;
  private final String appName;
  private final ApplicationSpecificationAdapter adapter;
  private final ArtifactRepository artifactRepository;

  /**
   * Constructor with hit for handling type.
   */
  public LocalArtifactLoaderStage(CConfiguration cConf, Store store, Id.Namespace namespace, @Nullable String appName,
                                  ArtifactRepository artifactRepository) {
    super(TypeToken.of(AppDeploymentInfo.class));
    this.cConf = cConf;
    this.store = store;
    this.namespace = namespace;
    this.appName = appName;
    this.adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    this.artifactRepository = artifactRepository;
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

    Id.Artifact artifactId = deploymentInfo.getArtifactId();
    Location artifactLocation = deploymentInfo.getArtifactLocation();
    String appClassName = deploymentInfo.getAppClassName();
    String configString = deploymentInfo.getConfigString();

    InMemoryConfigurator inMemoryConfigurator =
      new InMemoryConfigurator(cConf, artifactId, appClassName, artifactLocation, configString, artifactRepository);

    ListenableFuture<ConfigResponse> result = inMemoryConfigurator.config();
    ConfigResponse response = result.get(120, TimeUnit.SECONDS);
    if (response.getExitCode() != 0) {
      throw new IllegalArgumentException("Failed to configure application: " + deploymentInfo);
    }
    ApplicationSpecification specification = adapter.fromJson(response.get());
    if (appName != null) {
      specification = new ForwardingApplicationSpecification(specification) {
        @Override
        public String getName() {
          return appName;
        }
      };
    }

    Id.Application application = Id.Application.from(namespace, specification.getName());
    emit(new ApplicationDeployable(application, specification, store.getApplication(application),
      ApplicationDeployScope.USER, artifactLocation));
  }
}
