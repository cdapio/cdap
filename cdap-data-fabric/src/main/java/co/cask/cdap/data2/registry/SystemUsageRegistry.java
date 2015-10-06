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

package co.cask.cdap.data2.registry;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * {@link UsageRegistry} implementation that uses Constants.Bindings.SYSTEM_DATASET_FRAMEWORK named implementation of
 * {@link DatasetFramework}.
 */
public class SystemUsageRegistry extends UsageRegistry {

  @Inject
  public SystemUsageRegistry(TransactionExecutorFactory txExecutorFactory,
                             @Named(Constants.Bindings.SYSTEM_DATASET_FRAMEWORK) DatasetFramework datasetFramework) {
    super(txExecutorFactory, datasetFramework);

  }
}
