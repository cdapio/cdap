/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.WorkflowContext;
import com.google.common.base.Throwables;
import com.google.common.io.CharStreams;
import org.apache.tephra.TransactionFailureException;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
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
  static final String CUSTOM_FILESET = "customfs";
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
      addAction(new MyCustomAction());
    }

    @Override
    public void initialize(WorkflowContext context) throws Exception {
      super.initialize(context);

      try {
        // Try to load this workflow class from the context classloader
        // This is for validating CDAP-6035 that the context classloader is being set correctly
        Class<?> cls = Thread.currentThread().getContextClassLoader().loadClass(getClass().getName());
        Assert.assertSame(cls, getClass());
      } catch (ClassNotFoundException e) {
        throw Throwables.propagate(e);
      }
    }

    private static class TestAction extends AbstractCustomAction {
      @UseDataSet(CUSTOM_TABLE)
      private KeyValueTable table;

      @Override
      public void run() throws TransactionFailureException {
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext context) throws Exception {
            table.write("hello", "world");
          }
        });

        FileSet fs = getContext().getDataset(CUSTOM_FILESET);
        try (OutputStream out = fs.getLocation("test").getOutputStream()) {
          out.write(42);
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }

        URL serviceURL = getContext().getServiceURL(CUSTOM_SERVICE);
        if (serviceURL != null) {
          try {
            HttpURLConnection con = (HttpURLConnection) new URL(serviceURL, "service").openConnection();
            try (BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()))) {
              Assert.assertEquals("service", CharStreams.toString(in));
            } finally {
              con.disconnect();
            }
          } catch (IOException e) {
            throw Throwables.propagate(e);
          }
        }
      }
    }

    private static class MyCustomAction extends AbstractCustomAction {

      @Override
      public void run() throws Exception {
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext context) throws Exception {
            KeyValueTable table = context.getDataset(CUSTOM_TABLE);
            table.write("another.hello", "another.world");
          }
        });
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
