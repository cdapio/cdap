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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.http.HttpResponder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Mock ETL Endpoints.
 */
//TODO: Replace these with actual logic
@Path(Constants.Gateway.API_VERSION_3 + "/templates")
public class MockETLHandler extends AuthenticatedHttpHandler {

  private static final String REALTIME = "etlrealtime";
  private static final String BATCH = "etlbatch";

  @Inject
  public MockETLHandler(Authenticator authenticator) {
    super(authenticator);
  }

  @GET
  public void getTemplates(HttpRequest request, HttpResponder responder) throws Exception {
    JsonArray templates = new JsonArray();
    JsonObject realtime = new JsonObject();
    JsonObject batch = new JsonObject();
    realtime.addProperty("name", REALTIME);
    realtime.addProperty("description", "ETL Realtime Adapter");
    batch.addProperty("name", BATCH);
    batch.addProperty("description", "ETL Batch Adapter");
    templates.add(realtime);
    templates.add(batch);
    responder.sendJson(HttpResponseStatus.OK, templates);
  }

  @Path("/{template-id}")
  @GET
  public void getTemplate(HttpRequest request, HttpResponder responder, @PathParam("template-id") String templateId)
    throws Exception {
    JsonObject template = new JsonObject();
    JsonArray properties = new JsonArray();
    template.addProperty("name", templateId);
    template.addProperty("description", "Template used to create ETL Adapters");
    if (templateId.equalsIgnoreCase(BATCH)) {
      JsonObject scheduleProperty = new JsonObject();
      scheduleProperty.addProperty("name", "schedule");
      scheduleProperty.addProperty("description", "Cron Schedule for Batch Adapter");
      scheduleProperty.addProperty("required", true);
      properties.add(scheduleProperty);
    } else {
      JsonObject instanceProperty = new JsonObject();
      instanceProperty.addProperty("name", "instances");
      instanceProperty.addProperty("description", "Number of Instances");
      instanceProperty.addProperty("required", true);
      properties.add(instanceProperty);
    }
    template.add("properties", properties);
    responder.sendJson(HttpResponseStatus.OK, template);
  }

  @Path("/{template-id}/sources")
  @GET
  public void getSources(HttpRequest request, HttpResponder responder, @PathParam("template-id") String templateId)
    throws Exception {
    JsonArray sources = new JsonArray();
    if (templateId.equalsIgnoreCase(REALTIME)) {
      JsonObject kafka = new JsonObject();
      JsonObject jms = new JsonObject();
      JsonObject twitter = new JsonObject();
      kafka.addProperty("name", "Kafka");
      kafka.addProperty("description", "Kafka Source");
      jms.addProperty("name", "JMS");
      jms.addProperty("description", "JMS Source");
      twitter.addProperty("name", "Twitter");
      twitter.addProperty("description", "Twitter Source");
      sources.add(kafka);
      sources.add(jms);
      sources.add(twitter);
    } else {
      JsonObject db = new JsonObject();
      JsonObject stream = new JsonObject();
      db.addProperty("name", "KVTableSource");
      db.addProperty("description", "KeyValue Table Source");
      stream.addProperty("name", "Stream");
      stream.addProperty("description", "Stream Source");
      sources.add(db);
      sources.add(stream);
    }
    responder.sendJson(HttpResponseStatus.OK, sources);
  }

  @Path("/{template-id}/sinks")
  @GET
  public void getSinks(HttpRequest request, HttpResponder responder, @PathParam("template-id") String templateId)
    throws Exception {
    JsonArray sinks = new JsonArray();
    if (templateId.equalsIgnoreCase(REALTIME)) {
      JsonObject stream = new JsonObject();
      stream.addProperty("name", "Stream");
      stream.addProperty("description", "Stream Sink");
      sinks.add(stream);
    } else {
      JsonObject db = new JsonObject();
      db.addProperty("name", "KVTableSink");
      db.addProperty("description", "KeyValue Table Sink");
      sinks.add(db);
    }
    responder.sendJson(HttpResponseStatus.OK, sinks);
  }

  @Path("/{template-id}/transforms")
  @GET
  public void getTransforms(HttpRequest request, HttpResponder responder, @PathParam("template-id") String templateId)
    throws Exception {
    JsonArray transforms = new JsonArray();
    //TODO: Transforms will be shared between batch, realtime -> to be placed in both dir?
    JsonObject filter = new JsonObject();
    JsonObject projection = new JsonObject();
    filter.addProperty("name", "IdentityTransform");
    filter.addProperty("description", "Identity Transform");
    projection.addProperty("name", "ProjectionTransform");
    projection.addProperty("description", "Projection Transform");
    transforms.add(filter);
    transforms.add(projection);
    responder.sendJson(HttpResponseStatus.OK, transforms);
  }

  @Path("/{template-id}/sources/{source-id}")
  @GET
  public void getSource(HttpRequest request, HttpResponder responder, @PathParam("template-id") String templateId,
                        @PathParam("source-id") String sourceId) throws Exception {
    JsonObject transform = new JsonObject();
    JsonArray properties = new JsonArray();
    JsonObject nameProperty = new JsonObject();
    transform.addProperty("name", sourceId);
    transform.addProperty("description", "Data Source");
    nameProperty.addProperty("name", "name");
    nameProperty.addProperty("description", "Table Name");
    nameProperty.addProperty("required", true);
    properties.add(nameProperty);
    transform.add("properties", properties);
    responder.sendJson(HttpResponseStatus.OK, transform);
  }

  @Path("/{template-id}/transforms/{transform-id}")
  @GET
  public void getTransform(HttpRequest request, HttpResponder responder, @PathParam("template-id") String templateId,
                           @PathParam("transform-id") String transformId) throws Exception {
    JsonObject transform = new JsonObject();
    JsonArray properties = new JsonArray();
    JsonObject nameProperty = new JsonObject();
    transform.addProperty("name", transformId);
    transform.addProperty("description", "Data Transform");
    nameProperty.addProperty("name", "name");
    nameProperty.addProperty("description", "Name of the Data");
    nameProperty.addProperty("required", false);
    properties.add(nameProperty);
    transform.add("properties", properties);
    responder.sendJson(HttpResponseStatus.OK, transform);
  }

  @Path("/{template-id}/sinks/{sink-id}")
  @GET
  public void getSink(HttpRequest request, HttpResponder responder, @PathParam("template-id") String templateId,
                      @PathParam("sink-id") String sinkId) throws Exception {
    JsonObject transform = new JsonObject();
    JsonArray properties = new JsonArray();
    JsonObject nameProperty = new JsonObject();
    transform.addProperty("name", sinkId);
    transform.addProperty("description", "Data Sink");
    nameProperty.addProperty("name", "name");
    nameProperty.addProperty("description", "Table Name");
    nameProperty.addProperty("required", true);
    properties.add(nameProperty);
    transform.add("properties", properties);
    responder.sendJson(HttpResponseStatus.OK, transform);
  }
}
