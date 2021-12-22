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
 */

package io.cdap.cdap.master.environment.k8s;

import io.cdap.cdap.k8s.spi.environment.KubeEnvironmentInitializer;

import java.util.Collections;
import java.util.Map;

/**
 * Provides no initializers for the k8s environment.
 */
public class NoOpKubeEnvironmentInitializerProvider implements KubeEnvironmentInitializerProvider {
  @Override
  public Map<String, KubeEnvironmentInitializer> loadKubeEnvironmentInitializers() {
    return Collections.emptyMap();
  }
}
