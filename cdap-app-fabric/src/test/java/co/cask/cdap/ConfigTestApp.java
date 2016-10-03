/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.worker.AbstractWorker;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 *
 */
public class ConfigTestApp extends AbstractApplication<ConfigTestApp.ConfigClass> {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigTestApp.class);

  public static final String NAME = ConfigTestApp.class.getSimpleName();
  public static final String FLOW_NAME = "simpleFlow";
  public static final String FLOWLET_NAME = "simpleFlowlet";
  public static final String SERVICE_NAME = "simpleService";
  public static final String DEFAULT_RESPONSE = "defaultResponse";
  public static final String DEFAULT_STREAM = "defaultStream";
  public static final String DEFAULT_TABLE = "defaultTable";

  public static class ConfigClass extends Config {
    private String streamName;
    private String tableName;
    private String serviceResponse;

    public ConfigClass() {
      this.streamName = DEFAULT_STREAM;
      this.tableName = DEFAULT_TABLE;
      this.serviceResponse = DEFAULT_RESPONSE;
    }

    public ConfigClass(String streamName, String tableName, String serviceResponse) {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(streamName));
      Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName));
      Preconditions.checkArgument(!Strings.isNullOrEmpty(serviceResponse));
      this.streamName = streamName;
      this.tableName = tableName;
      this.serviceResponse = serviceResponse;
    }

    public ConfigClass(String streamName, String tableName) {
      this(streamName, tableName, DEFAULT_RESPONSE);
    }

    public String getStreamName() {
      return streamName;
    }

    public String getTableName() {
      return tableName;
    }

    public String getServiceResponse() {
      return serviceResponse;
    }
  }

  @Override
  public void configure() {
    setName(NAME);
    ConfigClass configObj = getConfig();
    addWorker(new DefaultWorker(configObj.streamName));
    addFlow(new SimpleFlow(configObj.streamName, configObj.tableName));
    addService(SERVICE_NAME, new SimpleHandler(configObj.getServiceResponse()));
  }

  public static class SimpleHandler extends AbstractHttpServiceHandler {
    @Property
    private final String response;

    public SimpleHandler(String response) {
      this.response = response;
    }

    @GET
    @Path("ping")
    public void handle(HttpServiceRequest request, HttpServiceResponder responder) throws IOException {
      responder.sendString(response);
    }
  }

  private static class SimpleFlow extends AbstractFlow {

    private final String streamName;
    private final String datasetName;

    public SimpleFlow(String streamName, String datasetName) {
      this.streamName = streamName;
      this.datasetName = datasetName;
    }

    @Override
    protected void configure() {
      setName(FLOW_NAME);
      addFlowlet(FLOWLET_NAME, new SimpleFlowlet(datasetName));
      connectStream(streamName, FLOWLET_NAME);
    }
  }

  private static class SimpleFlowlet extends AbstractFlowlet {

    @Property
    private final String datasetName;
    private String appName;

    public SimpleFlowlet(String datasetName) {
      this.datasetName = datasetName;
    }

    @ProcessInput
    public void process(StreamEvent event) {
      KeyValueTable dataset = getContext().getDataset(datasetName);
      String data = Bytes.toString(event.getBody());
      dataset.write(appName + "." + data, data);
    }

    @Override
    public void configure() {
      createDataset(datasetName, KeyValueTable.class);
    }

    @Override
    public void initialize(FlowletContext context) throws Exception {
      super.initialize(context);
      appName = context.getApplicationSpecification().getName();
    }
  }

  private static class DefaultWorker extends AbstractWorker {
    private final String streamName;
    private volatile boolean stopped;

    public DefaultWorker(String streamName) {
      this.streamName = streamName;
    }

    @Override
    public void run() {
      while (!stopped) {
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          LOG.error("Interrupted Exception", e);
        }

        try {
          getContext().write(streamName, "Hello World");
        } catch (IOException e) {
          LOG.error("IOException while trying to write to stream", e);
        }
      }
    }

    @Override
    protected void configure() {
      super.configure();
      addStream(new Stream(streamName));
    }

    @Override
    public void stop() {
      stopped = true;
    }
  }
}
