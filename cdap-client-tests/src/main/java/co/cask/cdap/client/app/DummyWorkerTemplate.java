/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.client.app;

import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.app.ApplicationContext;
import co.cask.cdap.api.templates.AdapterConfigurer;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.api.worker.AbstractWorker;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * App Template that has a Worker as the Program Type.
 */
public class DummyWorkerTemplate extends ApplicationTemplate<DummyWorkerTemplate.Config> {
  public static final String NAME = "workerTemplate";
  private static final String ADAPTER_NAME = "adapterName";

  @Override
  public void configure(ApplicationConfigurer configurer, ApplicationContext context) {
    configurer.setName(NAME);
    configurer.addWorker(new TWorker());
  }

  /**
   *
   */
  public static class Config {
    private final int instances;

    public Config(int instances) {
      this.instances = instances;
    }
  }

  @Override
  public void configureAdapter(String adapterName, Config config, AdapterConfigurer configurer) throws Exception {
    configurer.addRuntimeArgument(ADAPTER_NAME, adapterName);
    configurer.setInstances(config.instances);
  }

  /**
   *
   */
  public static class TWorker extends AbstractWorker {
    private static final Logger LOG = LoggerFactory.getLogger(TWorker.class);
    public static final String NAME = TWorker.class.getSimpleName();
    private volatile boolean running;

    @Override
    public void configure() {
      setName(NAME);
    }

    @Override
    public void run() {
      running = true;
      String adapterName = getContext().getRuntimeArguments().get(ADAPTER_NAME);
      int instanceCount = getContext().getInstanceCount();
      int instanceId = getContext().getInstanceId();
      Preconditions.checkNotNull(adapterName);
      while (running) {
        try {
          LOG.info("Adapter {}; Instance: Count {} Id {}", adapterName, instanceCount, instanceId);
          TimeUnit.MILLISECONDS.sleep(250);
        } catch (InterruptedException e) {
          // no-op
        }
      }
    }

    @Override
    public void stop() {
      running = false;
    }
  }
}
