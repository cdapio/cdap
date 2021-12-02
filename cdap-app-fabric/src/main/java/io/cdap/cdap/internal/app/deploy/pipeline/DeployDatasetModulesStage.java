/*
 * Copyright © 2014 Cask Data, Inc.
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
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.pipeline.AbstractStage;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.security.authorization.AuthorizationUtil;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;

import java.util.Map;

/**
 * This {@link io.cdap.cdap.pipeline.Stage} is responsible for automatic
 * deploy of the {@link DatasetModule}s specified by application.
 */
public class DeployDatasetModulesStage extends AbstractStage<ApplicationDeployable> {

  private final DatasetModulesDeployer deployer;
  private final OwnerAdmin ownerAdmin;
  private final AuthenticationContext authenticationContext;
  private final Impersonator impersonator;
  private final ArtifactRepository artifactRepository;
  private final boolean allowCustomModule;

  public DeployDatasetModulesStage(CConfiguration cConf,
                                   DatasetFramework datasetFramework, DatasetFramework inMemoryDatasetFramework,
                                   OwnerAdmin ownerAdmin, AuthenticationContext authenticationContext,
                                   ArtifactRepository artifactRepository, Impersonator impersonator) {
    super(TypeToken.of(ApplicationDeployable.class));
    this.deployer = new DatasetModulesDeployer(datasetFramework, inMemoryDatasetFramework, cConf);
    this.ownerAdmin = ownerAdmin;
    this.authenticationContext = authenticationContext;
    this.allowCustomModule = cConf.getBoolean(Constants.Dataset.CUSTOM_MODULE_ENABLED);
    this.impersonator = impersonator;
    this.artifactRepository = artifactRepository;
  }

  /**
   * Deploys dataset modules specified in the given application spec.
   *
   * @param input An instance of {@link ApplicationDeployable}
   */
  @Override
  public void process(ApplicationDeployable input) throws Exception {
    Map<String, String> datasetModules = input.getSpecification().getDatasetModules();

    if (allowCustomModule) {
      KerberosPrincipalId ownerPrincipal = input.getOwnerPrincipal();
      // get the authorizing user
      String authorizingUser =
        AuthorizationUtil.getAppAuthorizingUser(ownerAdmin, authenticationContext, input.getApplicationId(),
                                                ownerPrincipal);

      EntityImpersonator classLoaderImpersonator = new EntityImpersonator(input.getArtifactId(), impersonator);
      try (CloseableClassLoader classLoader =
             artifactRepository.createArtifactClassLoader(new ArtifactDescriptor(input.getArtifactId().getNamespace(),
                                                                                 input.getArtifactId().toApiArtifactId(),
                                                                                 input.getArtifactLocation()),
                                                          classLoaderImpersonator)) {
        deployer.deployModules(input.getApplicationId().getParent(),
                               datasetModules,
                               input.getArtifactLocation(),
                               classLoader, authorizingUser);
      }
    } else if (deployer.hasNonSystemDatasetModules(datasetModules)) {
      throw new IllegalStateException("Custom dataset module is not supported. " +
                                        "One of the dataset module is a custom module: " + datasetModules);
    }

    // Emit the input to next stage.
    emit(input);
  }
}
