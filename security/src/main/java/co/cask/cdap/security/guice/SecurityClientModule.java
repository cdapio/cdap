/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.security.guice;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.security.authorization.ACLClient;
import co.cask.cdap.security.authorization.RemoteACLClient;
import co.cask.cdap.security.authorization.RequiresPermissions;
import co.cask.cdap.security.authorization.RequiresPermissionsMethodInterceptor;
import com.google.inject.AbstractModule;
import com.google.inject.matcher.Matchers;

/**
 *
 */
public class SecurityClientModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(ACLClient.class).to(RemoteACLClient.class);
    bindInterceptor(Matchers.any(), Matchers.annotatedWith(RequiresPermissions.class),
                    new RequiresPermissionsMethodInterceptor(getProvider(ACLClient.class),
                                                             getProvider(CConfiguration.class)));
  }
}
