/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.metrics.guice;

import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.metrics.store.DefaultMetricDatasetFactory;
import co.cask.cdap.metrics.store.DefaultMetricStore;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.name.Named;

/**
 * Guice module for providing bindings for {@link MetricStore}.
 */
public final class MetricsStoreModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(MetricDatasetFactory.class).to(DefaultMetricDatasetFactory.class).in(Scopes.SINGLETON);
    bind(MetricStore.class).to(DefaultMetricStore.class);
    expose(MetricStore.class);
  }
}
