/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.common.namespace.guice;

import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.namespace.RemoteNamespaceQueryClient;

/**
 * Guice module to provide binding for {@link NamespaceQueryAdmin}.
 */
public class NamespaceQueryAdminModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(NamespaceQueryAdmin.class).to(RemoteNamespaceQueryClient.class).in(Scopes.SINGLETON);
    expose(NamespaceQueryAdmin.class);
  }
}
