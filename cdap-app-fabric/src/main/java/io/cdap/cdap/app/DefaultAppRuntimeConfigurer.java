/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.app;

import io.cdap.cdap.api.app.RuntimeConfigurer;
import io.cdap.cdap.app.services.AbstractServiceDiscoverer;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;

import java.util.Map;

/**
 * Default app configurer for runtime deployment
 */
public class DefaultAppRuntimeConfigurer extends AbstractServiceDiscoverer implements RuntimeConfigurer {
  private final RemoteClientFactory remoteClientFactory;
  private final Map<String, String> userProps;

  public DefaultAppRuntimeConfigurer(String namespace,
                                     RemoteClientFactory remoteClientFactory, Map<String, String> userProps) {
    super(namespace);
    this.remoteClientFactory = remoteClientFactory;
    this.userProps = userProps;
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return userProps;
  }

  @Override
  protected RemoteClientFactory getRemoteClientFactory() {
    return remoteClientFactory;
  }
}
