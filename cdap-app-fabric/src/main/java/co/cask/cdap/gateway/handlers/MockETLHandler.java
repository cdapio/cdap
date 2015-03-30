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
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Mock ETL Endpoints.
 */
//TODO: Replace these with actual logic
@Path(Constants.Gateway.API_VERSION_3 + "/templates")
public class MockETLHandler extends AuthenticatedHttpHandler {

  public MockETLHandler(Authenticator authenticator) {
    super(authenticator);
  }

  @Path("/")
  @GET
  public void getTemplates(HttpRequest request, HttpResponder responder) throws Exception {
    List<Map<String, String>> templates = Lists.newArrayList();
    templates.add(ImmutableMap.of("name", "etl.realtime", "description", "ETL Realtime Adapter"));
    templates.add(ImmutableMap.of("name", "etl.batch", "description", "ETL Batch Adapter"));
    responder.sendJson(HttpResponseStatus.OK, templates);
  }

  @Path("/{template-id}")
  @GET
  public void getTemplate(HttpRequest request, HttpResponder responder, @PathParam("template-id") String templateId)
    throws Exception {
    Map<String, String> template = ImmutableMap.of("name", templateId,
                                                   "description", "Template used to create ETL Adapters");
    responder.sendJson(HttpResponseStatus.OK, template);
  }

  @Path("/{template-id}/sources")
  @GET
  public void getSources(HttpRequest request, HttpResponder responder, @PathParam("template-id") String templateId)
    throws Exception {
    List<Map<String, String>> sources = Lists.newArrayList();
    if (templateId.equalsIgnoreCase("etl.realtime")) {
      sources.add(ImmutableMap.of("name", "Kafka", "description", "Kafka Source"));
      sources.add(ImmutableMap.of("name", "JMS", "description", "JMS Source"));
      sources.add(ImmutableMap.of("name", "Twitter", "description", "Twitter Source"));
    } else {
      sources.add(ImmutableMap.of("name", "DB", "description", "RDBMS Source"));
      sources.add(ImmutableMap.of("name", "Stream", "description", "Stream Source"));
    }
    responder.sendJson(HttpResponseStatus.OK, sources);
  }

  @Path("/{template-id}/sources")
  @GET
  public void getSinks(HttpRequest request, HttpResponder responder, @PathParam("template-id") String templateId)
    throws Exception {
    List<Map<String, String>> sources = Lists.newArrayList();
    if (templateId.equalsIgnoreCase("etl.realtime")) {
      sources.add(ImmutableMap.of("name", "Stream", "description", "Stream Sink"));
    } else {
      sources.add(ImmutableMap.of("name", "DB", "description", "RDBMS Sink"));
    }
    responder.sendJson(HttpResponseStatus.OK, sources);
  }

  @Path("/{template-id}/transforms")
  public void getTransforms(HttpRequest request, HttpResponder responder, @PathParam("template-id") String templateId)
    throws Exception {
    List<Map<String, String>> transforms = Lists.newArrayList();
    //TODO: Transforms will be shared between etl.batch, etl.realtime -> to be placed in both dir?
    transforms.add(ImmutableMap.of("name", "FilterTransform", "description", "Filter Data"));
    transforms.add(ImmutableMap.of("name", "ProjectionTransform", "description", "Projection Transform"));
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
    nameProperty.addProperty("name", "data.name");
    nameProperty.addProperty("description", "Name of the Data");
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
    nameProperty.addProperty("name", "data.name");
    nameProperty.addProperty("description", "Name of the Data");
    nameProperty.addProperty("required", true);
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
    nameProperty.addProperty("name", "data.name");
    nameProperty.addProperty("description", "Name of the Data");
    nameProperty.addProperty("required", true);
    properties.add(nameProperty);
    transform.add("properties", properties);
    responder.sendJson(HttpResponseStatus.OK, transform);
  }
}
