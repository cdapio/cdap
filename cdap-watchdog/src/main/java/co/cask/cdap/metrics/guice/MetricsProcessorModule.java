/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.metrics.process.AggregatesMetricsProcessor;
import co.cask.cdap.metrics.process.MetricsProcessor;
import co.cask.cdap.metrics.process.TimeSeriesMetricsProcessor;
import co.cask.cdap.metrics.transport.MetricsRecord;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;

/**
 * Guice module for creating bindings for {@link MetricsProcessor}. Intends to be installed by other modules,
 * preferably private module.
 */
public final class MetricsProcessorModule extends AbstractModule {

  private final Predicate<MetricsRecord> aggregatesFilter;

  public MetricsProcessorModule() {
    this(Predicates.<MetricsRecord>alwaysTrue());
  }

  /**
   * Creates a module with the given filter used for filtering MetricsRecord to get aggregated.
   */
  public MetricsProcessorModule(Predicate<MetricsRecord> filter) {
    this.aggregatesFilter = filter;
  }


  @Override
  protected void configure() {
    bind(new TypeLiteral<Predicate<MetricsRecord>>() { })
      .annotatedWith(Names.named("metrics.aggregates.predicate"))
      .toInstance(aggregatesFilter);

    Multibinder<MetricsProcessor> processorBinder = Multibinder.newSetBinder(binder(), MetricsProcessor.class);

    processorBinder.addBinding().to(TimeSeriesMetricsProcessor.class).in(Scopes.SINGLETON);
    processorBinder.addBinding().to(AggregatesMetricsProcessor.class).in(Scopes.SINGLETON);
  }
}
