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

import co.cask.cdap.api.Config;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletConfigurer;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.worker.AbstractWorker;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;


/**
 * Test Application that will register different programs based on the config.
 */
public class ConfigurableProgramsApp2 extends AbstractApplication<ConfigurableProgramsApp2.Programs> {

  /**
   * Application Config Class.
   */
  public static class Programs extends Config {
    @Nullable
    private String flow;
    @Nullable
    private String worker;
    @Nullable
    private String stream;
    @Nullable
    private String dataset;
    @Nullable
    private String service;

    public Programs() {
      this.stream = "streem";
      this.dataset = "dutaset";
    }

    public Programs(String flow, String worker, String stream, String dataset, String service) {
      this.flow = flow;
      this.worker = worker;
      this.stream = stream;
      this.dataset = dataset;
      this.service = service;
    }
  }

  @Override
  public void configure() {
    Programs config = getConfig();
    if (config.flow != null) {
      addFlow(new Floh(config.flow, config.stream, config.dataset));
    }
    if (config.worker != null) {
      addWorker(new Wurker(config.stream));
    }
    if (config.service != null) {
      addService(config.service, new PingService());
    }
  }

  private static class Floh extends AbstractFlow {
    private final String name;
    private final String stream;
    private final String dataset;

    public Floh(String name, String stream, String dataset) {
      this.name = name;
      this.stream = stream;
      this.dataset = dataset;
    }

    @Override
    protected void configureFlow() {
      setName(name);
      addFlowlet("flohlet", new Flohlet(dataset));
      connectStream(stream, "flohlet");
    }
  }

  private static class Flohlet extends AbstractFlowlet {

    @Property
    private final String datasetName;

    private KeyValueTable keyValueTable;

    public Flohlet(String datasetName) {
      this.datasetName = datasetName;
    }

    @ProcessInput
    public void process(StreamEvent event) {
      String data = Bytes.toString(event.getBody());
      String[] fields = data.split(",");
      keyValueTable.write(fields[0], fields[1]);
    }

    @Override
    public void configure(FlowletConfigurer configurer) {
      super.configure(configurer);
      useDatasets(datasetName);
    }

    @Override
    public void initialize(FlowletContext context) throws Exception {
      super.initialize(context);
      keyValueTable = context.getDataset(datasetName);
    }
  }

  private static class Wurker extends AbstractWorker {
    private final String streamName;
    private volatile boolean running;

    public Wurker(String streamName) {
      this.streamName = streamName;
    }

    @Override
    public void run() {
      running = true;
      while (running) {
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          // shouldn't happen in test
        }

        try {
          getContext().write(streamName, "Samuel,L. Jackson");
          getContext().write(streamName, "Dwayne,Johnson");
        } catch (IOException e) {
          // shouldn't happen in test
        }
      }
    }

    @Override
    public void stop() {
      running = false;
    }
  }
}
