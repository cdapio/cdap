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

package io.cdap.cdap.data2.dataset2;

import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.api.dataset.module.DatasetDefinitionRegistry;

/**
 * Default implementation of {@link DatasetDefinitionRegistryFactory} that creates instance
 * of {@link DatasetDefinitionRegistry} using {@link DefaultDatasetDefinitionRegistry} as implementation.
 */
public class DefaultDatasetDefinitionRegistryFactory implements DatasetDefinitionRegistryFactory {

  private final Injector injector;

  @Inject
  public DefaultDatasetDefinitionRegistryFactory(Injector injector) {
    this.injector = injector;
  }

  @Override
  public DatasetDefinitionRegistry create() {
    DefaultDatasetDefinitionRegistry registry = new DefaultDatasetDefinitionRegistry();
    injector.injectMembers(registry);
    return registry;
  }
}
