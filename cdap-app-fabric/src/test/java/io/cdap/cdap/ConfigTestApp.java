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

package io.cdap.cdap;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.annotation.Property;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.workflow.AbstractWorkflow;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 *
 */
public class ConfigTestApp extends AbstractApplication<ConfigTestApp.ConfigClass> {

  public static final String NAME = ConfigTestApp.class.getSimpleName();
  public static final String SCHEDULE_NAME = "scheduleName";
  public static final String WORKFLOW_NAME = "defaultWorkflow";
  public static final String SERVICE_NAME = "simpleService";

  public static final String DEFAULT_RESPONSE = "defaultResponse";
  public static final String DEFAULT_TABLE = "defaultTable";

  public static class ConfigClass extends Config {
    private String tableName;
    private String serviceResponse;

    public ConfigClass() {
      this(DEFAULT_TABLE, DEFAULT_RESPONSE);
    }

    public ConfigClass(String tableName) {
      this(tableName, DEFAULT_RESPONSE);
    }

    public ConfigClass(String tableName, String serviceResponse) {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName));
      Preconditions.checkArgument(!Strings.isNullOrEmpty(serviceResponse));
      this.tableName = tableName;
      this.serviceResponse = serviceResponse;
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
    addService(SERVICE_NAME, new SimpleHandler(configObj.getServiceResponse(), configObj.getTableName()));
    addWorkflow(new DefaultWorkflow());
    schedule(buildSchedule(SCHEDULE_NAME, ProgramType.WORKFLOW, WORKFLOW_NAME)
              .triggerOnProgramStatus(ProgramType.WORKFLOW, WORKFLOW_NAME, ProgramStatus.FAILED));

    createDataset(configObj.getTableName(), KeyValueTable.class);
  }

  public static class SimpleHandler extends AbstractHttpServiceHandler {

    @Property
    private final String response;
    @Property
    private final String tableName;

    public SimpleHandler(String response, String tableName) {
      this.response = response;
      this.tableName = tableName;
    }

    @GET
    @Path("ping")
    public void handle(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendString(response);
    }

    @PUT
    @Path("write/{data}")
    public void write(HttpServiceRequest request, HttpServiceResponder responder,
                      @PathParam("data") String data) {
      String appName = getContext().getApplicationSpecification().getName();
      KeyValueTable dataset = getContext().getDataset(tableName);
      dataset.write(appName + "." + data, data);
      responder.sendStatus(200);
    }
  }

  private static class DefaultWorkflow extends AbstractWorkflow {
    @Override
    protected void configure() {
      setName(WORKFLOW_NAME);
    }
  }
}
