package com.continuuity.internal.app.runtime.batch.distributed;

import com.continuuity.app.program.Program;
import com.continuuity.app.program.Programs;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.DistributedDataSetAccessor;
import com.continuuity.internal.app.runtime.batch.AbstractMapReduceContextBuilder;
import com.continuuity.logging.appender.LogAppender;
import com.continuuity.logging.appender.kafka.KafkaLogAppender;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;

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

  @Override
  protected Program loadProgram(URI programLocation, LocationFactory locationFactory) throws IOException {
    return Programs.create(locationFactory.create(programLocation));
  }

  protected Injector createInjector() {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new LocationRuntimeModule().getDistributedModules(),
      new IOModule(),
      new MetricsClientRuntimeModule().getNoopModules(),
      new AbstractModule() {
        @Override
        protected void configure() {

          // Data-fabric bindings
          bind(Configuration.class).annotatedWith(Names.named("HBaseOVCTableHandleHConfig")).to(Configuration.class);
          bind(CConfiguration.class).annotatedWith(Names.named("HBaseOVCTableHandleCConfig")).to(CConfiguration.class);

          // txds2
          bind(DataSetAccessor.class).to(DistributedDataSetAccessor.class).in(Singleton.class);

          // For log publishing
          bind(LogAppender.class).to(KafkaLogAppender.class);
        }
      }
    );
  }
}
