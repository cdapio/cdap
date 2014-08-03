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

package co.cask.cdap.data.runtime;

import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

/**
 * Binds the {@link co.cask.cdap.common.metrics.MetricsCollectionService} to a no-op implementation.
 */
// TODO: replace the common MetricsCollectionService interface with a transactions specific one
public class TransactionMetricsModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Singleton.class);
  }
}
