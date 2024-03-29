/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.pipeline.AbstractStage;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.authorization.AuthorizationUtil;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;

/**
 * This {@link io.cdap.cdap.pipeline.Stage} is responsible for automatic deploy of the {@link
 * io.cdap.cdap.api.dataset.module.DatasetModule}s specified by application.
 */
public class CreateDatasetInstancesStage extends AbstractStage<ApplicationDeployable> {

  private final DatasetInstanceCreator datasetInstanceCreator;
  private final OwnerAdmin ownerAdmin;
  private final AuthenticationContext authenticationContext;

  public CreateDatasetInstancesStage(CConfiguration configuration,
      DatasetFramework datasetFramework,
      OwnerAdmin ownerAdmin, AuthenticationContext authenticationContext) {
    super(TypeToken.of(ApplicationDeployable.class));
    this.datasetInstanceCreator = new DatasetInstanceCreator(configuration, datasetFramework);
    this.ownerAdmin = ownerAdmin;
    this.authenticationContext = authenticationContext;
  }

  /**
   * Receives an input containing application specification and location and verifies both.
   *
   * @param input An instance of {@link ApplicationDeployable}
   */
  @Override
  public void process(ApplicationDeployable input) throws Exception {
    // create dataset instances
    ApplicationSpecification specification = input.getSpecification();
    NamespaceId namespaceId = input.getApplicationId().getParent();
    KerberosPrincipalId ownerPrincipal = input.getOwnerPrincipal();
    // get the authorizing user
    String authorizingUser =
        AuthorizationUtil.getAppAuthorizingUser(ownerAdmin, authenticationContext,
            input.getApplicationId(),
            ownerPrincipal);
    datasetInstanceCreator.createInstances(namespaceId, specification.getDatasets(), ownerPrincipal,
        authorizingUser);

    // Emit the input to next stage.
    emit(input);
  }
}
