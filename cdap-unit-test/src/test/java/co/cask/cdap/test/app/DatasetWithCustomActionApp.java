/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.test.app;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import co.cask.cdap.api.workflow.WorkflowActionConfigurer;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * App which copies data from one KVTable to another using a Workflow Custom Action.
 */
public class DatasetWithCustomActionApp extends AbstractApplication {
  static final String CUSTOM_TABLE = "customtable";
  static final String CUSTOM_TABLE1 = "customtable1";
  static final String CUSTOM_PROGRAM = "DatasetWithCustomActionApp";
  static final String CUSTOM_WORKFLOW = "CustomWorkflow";
  static final String CUSTOM_SERVICE = "CustomService";

  @Override
  public void configure() {
    setName(CUSTOM_PROGRAM);
    addWorkflow(new CustomWorkflow());
    addService(new CustomService());
  }

  public static class CustomWorkflow extends AbstractWorkflow {
    @Override
    protected void configure() {
      setName(CUSTOM_WORKFLOW);
      addAction(new TestAction());
    }

    private static class TestAction extends AbstractWorkflowAction {
      @UseDataSet(CUSTOM_TABLE)
      private KeyValueTable table;

      @Override
      public void configure(WorkflowActionConfigurer configurer) {
        super.configure(configurer);
        useDatasets(CUSTOM_TABLE1);
      }

      @Override
      public void run() {
        table.write("hello", "world");

        KeyValueTable table1 = getContext().getDataset(CUSTOM_TABLE1);
        table1.write("test", "another");

        URL serviceURL = getContext().getServiceURL(CUSTOM_SERVICE);
        if (serviceURL != null) {
          BufferedReader in = null;
          try {
            HttpURLConnection con =
              (HttpURLConnection) new URL(serviceURL, String.format("service")).openConnection();
            con.setRequestMethod("GET");

            in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            StringBuffer response = new StringBuffer();
            String line;
            while ((line = in.readLine()) != null) {
              response.append(line);
            }

          } catch (IOException e) {
            e.printStackTrace();
          } finally {
            if (in != null) {
              try {
                in.close();
              } catch (IOException e) {
                e.printStackTrace();
              }
            }
          }
        }
      }
    }
  }

  public static class CustomService extends AbstractService {
    @Override
    protected void configure() {
      setName(CUSTOM_SERVICE);
      addHandler(new CustomHandler());
    }

    public static class CustomHandler extends AbstractHttpServiceHandler {
      @UseDataSet(CUSTOM_TABLE)
      private KeyValueTable table;

      @GET
      @Path("{name}")
      public void sayHi(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("name") String name) {
        table.write("hi", name);
        responder.sendString(name);
      }
    }
  }
}
