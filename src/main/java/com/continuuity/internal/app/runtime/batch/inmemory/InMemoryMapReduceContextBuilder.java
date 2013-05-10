package com.continuuity.internal.app.runtime.batch.inmemory;

import com.continuuity.app.guice.BigMamaModule;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.internal.app.runtime.batch.AbstractMapReduceContextBuilder;
import com.continuuity.runtime.MetadataModules;
import com.continuuity.runtime.MetricsModules;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.hadoop.conf.Configuration;

/**
 * Builds an instance of {@link com.continuuity.internal.app.runtime.batch.BasicMapReduceContext} good for
 * in-memory environment
 */
public class InMemoryMapReduceContextBuilder extends AbstractMapReduceContextBuilder {
  private final CConfiguration cConf;

  public InMemoryMapReduceContextBuilder(CConfiguration cConf) {
    this.cConf = cConf;
  }

  protected Injector createInjector() {
    ImmutableList<Module> inMemoryModules = ImmutableList.of(
      new BigMamaModule(cConf),
      new MetricsModules().getInMemoryModules(),
      new DataFabricModules().getSingleNodeModules(),
      new MetadataModules().getInMemoryModules() // todo: do we need that?
    );

    return Guice.createInjector(inMemoryModules);
  }
}
