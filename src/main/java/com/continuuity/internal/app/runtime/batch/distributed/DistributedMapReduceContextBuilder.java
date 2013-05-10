package com.continuuity.internal.app.runtime.batch.distributed;

import com.continuuity.app.guice.BigMamaModule;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.internal.app.runtime.batch.AbstractMapReduceContextBuilder;
import com.continuuity.runtime.MetadataModules;
import com.continuuity.runtime.MetricsModules;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.hadoop.conf.Configuration;

/**
 * Builds an instance of {@link com.continuuity.internal.app.runtime.batch.BasicMapReduceContext} good for
 * distributed environment. The context is to be used in remote worker (e.g. Mapper task started in YARN container)
 */
public class DistributedMapReduceContextBuilder extends AbstractMapReduceContextBuilder {
  private final CConfiguration cConf;
  private final Configuration hConf;

  public DistributedMapReduceContextBuilder(CConfiguration cConf, Configuration hConf) {
    this.cConf = cConf;
    this.hConf = hConf;
  }

  protected Injector createInjector() {
    ImmutableList<Module> modules = ImmutableList.of(
      new BigMamaModule(cConf),
      new MetricsModules().getSingleNodeModules(),
      new DataFabricModules().getDistributedModules(),
      new MetadataModules().getSingleNodeModules(),
      new Module() {
        @Override
        public void configure(Binder binder) {
          binder.bind(Configuration.class).toInstance(hConf);
        }
      }
    );

    return Guice.createInjector(modules);
  }
}
