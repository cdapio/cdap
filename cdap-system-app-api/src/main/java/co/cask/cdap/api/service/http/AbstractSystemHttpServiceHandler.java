/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.api.service.http;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.spi.data.table.StructuredTableSpecification;

/**
 * Abstract class for system http service handlers. System handlers can only be used in applications that are deployed
 * in the system namespace.
 */
@Beta
public class AbstractSystemHttpServiceHandler extends
  AbstractHttpServiceHandler<SystemHttpServiceContext, SystemHttpServiceConfigurer> {

  /**
   * Create a system table that conforms to the given table specification when the
   * application is deployed. If the table already exists, nothing happens.
   *
   * @throws UnsupportedOperationException if the application is not a system application
   */
  protected void createTable(StructuredTableSpecification tableSpecification) {
    getConfigurer().createTable(tableSpecification);
  }
}
