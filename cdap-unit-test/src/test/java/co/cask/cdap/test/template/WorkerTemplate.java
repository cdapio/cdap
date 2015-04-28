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

package co.cask.cdap.test.template;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.app.ApplicationContext;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.templates.AdapterConfigurer;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.worker.WorkerContext;
import com.google.gson.Gson;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Application Template that uses a worker to write some data to a dataset for testing purposes.
 */
public class WorkerTemplate extends ApplicationTemplate<WorkerTemplate.Config> {
  private static final Gson GSON = new Gson();
  public static final String NAME = "workertemplate";

  public static class Config {
    private final String dsName;
    private final String functionName;
    private final long x;

    public Config(String dsName, String functionName, long x) {
      this.dsName = dsName;
      this.functionName = functionName;
      this.x = x;
    }
  }

  @Override
  public void configure(ApplicationConfigurer configurer, ApplicationContext context) {
    configurer.setName(NAME);
    configurer.addWorker(new Worker());
  }

  @Override
  public void configureAdapter(String name, @Nullable Config config, AdapterConfigurer configurer) throws Exception {
    String pluginId = "plugin123";
    configurer.usePlugin("callable", config.functionName, pluginId,
      PluginProperties.builder()
        .add("x", String.valueOf(config.x))
        .build());
    configurer.addRuntimeArgument("pluginId", pluginId);
    configurer.addRuntimeArgument("config", GSON.toJson(config));
    configurer.createDataset(config.dsName, KeyValueTable.class.getName(), DatasetProperties.EMPTY);
  }

  public static class Worker extends AbstractWorker {
    public static final String NAME = "worker";
    private Config config;
    private volatile boolean running;
    private Callable<Long> plugin;

    @Override
    public void initialize(WorkerContext context) throws Exception {
      super.initialize(context);
      config = GSON.fromJson(context.getRuntimeArguments().get("config"), Config.class);
      String pluginId = context.getRuntimeArguments().get("pluginId");
      plugin = context.newPluginInstance(pluginId);
      running = true;
    }

    @Override
    public void configure() {
      setName(NAME);
    }

    @Override
    public void run() {
      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          long y = plugin.call();
          KeyValueTable table = context.getDataset(config.dsName);
          table.write(Bytes.toBytes(config.x), Bytes.toBytes(y));
        }
      });
      while (running) {
        try {
          TimeUnit.MILLISECONDS.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public void stop() {
      running = false;
    }
  }

}
