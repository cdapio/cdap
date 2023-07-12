/*
 * Copyright © 2022-2023 Cask Data, Inc.
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

package io.cdap.cdap.app.guice;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import io.cdap.cdap.api.metrics.MetricsPublisher;
import io.cdap.cdap.metrics.jmx.JmxMetricsCollector;
import io.cdap.cdap.metrics.jmx.JmxMetricsCollectorFactory;
import io.cdap.cdap.metrics.process.loader.MetricsWriterExtensionLoader;
import io.cdap.cdap.metrics.process.loader.MetricsWriterProvider;
import io.cdap.cdap.metrics.publisher.MetricsWritersMetricsPublisher;

/**
 * Guice module that provides mappings for SystemMetricsExporterServiceMain.
 */
public class SystemMetricsExporterModule extends AbstractModule {

  @Override
  protected void configure() {
    install(new FactoryModuleBuilder()
        .implement(JmxMetricsCollector.class, JmxMetricsCollector.class)
        .build(JmxMetricsCollectorFactory.class));
    bind(MetricsPublisher.class).to(MetricsWritersMetricsPublisher.class);
    bind(MetricsWriterProvider.class).to(MetricsWriterExtensionLoader.class);
  }
}
