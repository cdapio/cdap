/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.client.app;

import com.google.common.base.Throwables;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.worker.AbstractWorker;
import io.cdap.cdap.api.workflow.AbstractWorkflow;
import org.apache.tephra.TransactionFailureException;

import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;


/**
 * Test Application that will register different programs based on the config.
 */
public class ConfigurableProgramsApp2 extends AbstractApplication<ConfigurableProgramsApp2.Programs> {

  /**
   * Application Config Class.
   */
  public static class Programs extends Config {
    @Nullable
    private String worker;
    @Nullable
    private String dataset;
    @Nullable
    private String service;
    @Nullable
    private String workflow;

    public Programs() {
      this.dataset = "dutaset";
    }

    public Programs(String worker, String service, String workflow, String dataset) {
      this.worker = worker;
      this.service = service;
      this.workflow = workflow;
      this.dataset = dataset;
    }
  }

  @Override
  public void configure() {
    ConfigurableProgramsApp2.Programs config = getConfig();
    if (config.worker != null) {
      addWorker(new ConfigurableProgramsApp2.Wurker(config.worker, config.dataset));
    }
    if (config.service != null) {
      addService(config.service, new ConfigurableProgramsApp2.PingHandler());
    }
    if (config.workflow != null) {
      addWorkflow(new TestWorkflow(config.workflow));
    }
  }

  /**
   * Test Workflow.
   */
  public static final class TestWorkflow extends AbstractWorkflow {

    private String name;

    TestWorkflow(String name) {
      this.name = name;
    }

    @Override
    protected void configure() {
      setName(name);
    }
  }

  /**
   * Test handler.
   */
  public static final class PingHandler extends AbstractHttpServiceHandler {

    @GET
    @Path("/ping")
    public void ping(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendStatus(200);
    }
  }

  private static class Wurker extends AbstractWorker {

    private final String workerName;
    private final String datasetName;

    Wurker(String workerName, String datasetName) {
      this.workerName = workerName;
      this.datasetName = datasetName;
    }

    @Override
    protected void configure() {
      setName(workerName);
    }

    @Override
    public void run() {
      try {
        getContext().execute(context -> {
          KeyValueTable keyValueTable = context.getDataset(datasetName);
          keyValueTable.write("Samuel", "L. Jackson");
          keyValueTable.write("Dwayne", "Johnson");
        });
      } catch (TransactionFailureException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
