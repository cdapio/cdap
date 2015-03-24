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

package co.cask.cdap.api.templates;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.service.http.HttpServiceHandler;

import javax.annotation.Nullable;

/**
 * Abstract App Template that provides additional functionality required for App Templates.
 */
@Beta
public abstract class ApplicationTemplate extends AbstractApplication {

  /**
   * Given the manifest configuration, provides the {@link ManifestSpecification}
   *
   * @param configuration manifest configuration
   * @return {@link ManifestSpecification}
   * @throws Exception if the configuration is not valid
   */
  public abstract ManifestSpecification configureManifest(String configuration) throws Exception;

  /**
   * Provide a Service Handler class that provides HTTP endpoints
   *
   * @return {@link HttpServiceHandler}
   */
  @Nullable
  public Class<? extends HttpServiceHandler> getServiceHandler() {
    return null;
  }
}
