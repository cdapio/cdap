/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.internal.app.authorization;

import co.cask.cdap.app.authorization.AuthorizationFactory;
import co.cask.cdap.app.authorization.AuthorizationHandler;
import co.cask.cdap.common.conf.CConfiguration;

/**
 *
 */
public class PassportAuthorizationFactory implements AuthorizationFactory {

  /**
   * Creates an instance of {@link co.cask.cdap.app.authorization.AuthorizationHandler} for authorizing requests
   * being processed by any service.
   *
   * @param configuration An instance of {@link co.cask.cdap.common.conf.CConfiguration} to configure.
   * @return An instance of {@link co.cask.cdap.app.authorization.AuthorizationHandler}
   */
  @Override
  public AuthorizationHandler create(CConfiguration configuration) {
    return new PassportAuthorizationHandler(configuration);
  }
}
