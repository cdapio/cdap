/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.guice;

import com.continuuity.metrics.process.AggregatesMetricsProcessor;
import com.continuuity.metrics.process.MetricsProcessor;
import com.continuuity.metrics.process.TimeSeriesMetricsProcessor;
import com.continuuity.metrics.transport.MetricsRecord;
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
