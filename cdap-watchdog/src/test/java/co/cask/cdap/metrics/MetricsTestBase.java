/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.metrics;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.metrics.MetricValues;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.io.DatumWriter;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.internal.io.ASMDatumWriterFactory;
import co.cask.cdap.internal.io.ASMFieldAccessorFactory;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.guice.MessagingServerRuntimeModule;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * TestBase for testing {@link co.cask.cdap.metrics.collect.MessagingMetricsCollectionService} 
 * and {@link co.cask.cdap.metrics.process.MessagingMetricsProcessorService}
 */
public abstract class MetricsTestBase {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  protected static Injector injector;
  protected static final String TOPIC_PREFIX = "metrics";

  protected static CConfiguration cConf;
  protected static MessagingService messagingService;
  protected static TypeToken<MetricValues> metricValueType;
  protected static Schema schema;
  protected static DatumWriter<MetricValues> recordWriter;

  @Before
  public void init() throws IOException, UnsupportedTypeException {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    injector = Guice.createInjector(getModules());
    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }
    metricValueType = TypeToken.of(MetricValues.class);
    schema = new ReflectionSchemaGenerator().generate(metricValueType.getType());
    recordWriter = new ASMDatumWriterFactory(new ASMFieldAccessorFactory())
      .create(metricValueType, schema);
  }

  @After
  public void stop() throws Exception {
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
  }

  private List<Module> getModules() {
    List<Module> modules = new ArrayList<>();
    modules.add(new ConfigModule(cConf));
    modules.add(new DiscoveryRuntimeModule().getInMemoryModules());
    modules.add(new MessagingServerRuntimeModule().getInMemoryModules());
    modules.add(new AbstractModule() {
      @Override
      protected void configure() {
        bind(MetricsCollectionService.class).toInstance(new NoOpMetricsCollectionService());
      }
    });
    modules.addAll(getAdditionalModules());
    return modules;
  }

  protected abstract List<Module> getAdditionalModules();
}
