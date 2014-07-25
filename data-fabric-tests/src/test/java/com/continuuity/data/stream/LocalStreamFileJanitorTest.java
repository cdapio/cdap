/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.data.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.file.FileWriter;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.continuuity.data.runtime.TransactionMetricsModule;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 *
 */
public class LocalStreamFileJanitorTest extends StreamFileJanitorTestBase {

  private static CConfiguration cConf;
  private static LocationFactory locationFactory;
  private static StreamAdmin streamAdmin;
  private static StreamFileWriterFactory fileWriterFactory;

  @BeforeClass
  public static void init() throws IOException {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new LocationRuntimeModule().getInMemoryModules(),
      new TransactionMetricsModule(),
      Modules.override(new DataFabricLevelDBModule()).with(new AbstractModule() {

        @Override
        protected void configure() {
          bind(StreamAdmin.class).to(TestStreamFileAdmin.class).in(Scopes.SINGLETON);
        }
      })
    );

    locationFactory = injector.getInstance(LocationFactory.class);
    streamAdmin = injector.getInstance(StreamAdmin.class);
    fileWriterFactory = injector.getInstance(StreamFileWriterFactory.class);
  }

  @Override
  protected LocationFactory getLocationFactory() {
    return locationFactory;
  }

  @Override
  protected StreamAdmin getStreamAdmin() {
    return streamAdmin;
  }

  @Override
  protected CConfiguration getCConfiguration() {
    return cConf;
  }

  @Override
  protected FileWriter<StreamEvent> createWriter(String streamName) throws IOException {
    StreamConfig config = streamAdmin.getConfig(streamName);
    return fileWriterFactory.create(config, StreamUtils.getGeneration(config));
  }
}
