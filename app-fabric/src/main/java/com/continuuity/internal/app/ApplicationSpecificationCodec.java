/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app;

import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.DatasetInstanceCreationSpec;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.app.ApplicationSpecification;
import com.google.common.collect.Maps;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.internal.json.TwillSpecificationAdapter;

import java.lang.reflect.Type;
import java.util.Map;

/**
 *
 */
final class ApplicationSpecificationCodec extends AbstractSpecificationCodec<ApplicationSpecification> {

  private final TwillSpecificationAdapter adapter;
  public ApplicationSpecificationCodec() {
    adapter = TwillSpecificationAdapter.create();
  }

  @Override
  public JsonElement serialize(ApplicationSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();

    jsonObj.add("name", new JsonPrimitive(src.getName()));
    jsonObj.add("description", new JsonPrimitive(src.getDescription()));
    jsonObj.add("streams", serializeMap(src.getStreams(), context, StreamSpecification.class));
    jsonObj.add("datasets", serializeMap(src.getDataSets(), context, DataSetSpecification.class));
    jsonObj.add("datasetModules", serializeMap(src.getDatasetModules(), context, String.class));
    jsonObj.add("datasetInstances", serializeMap(src.getDatasets(), context, DatasetInstanceCreationSpec.class));
    jsonObj.add("flows", serializeMap(src.getFlows(), context, FlowSpecification.class));
    jsonObj.add("procedures", serializeMap(src.getProcedures(), context, ProcedureSpecification.class));
    jsonObj.add("mapReduces", serializeMap(src.getMapReduce(), context, MapReduceSpecification.class));
    jsonObj.add("workflows", serializeMap(src.getWorkflows(), context, WorkflowSpecification.class));
    jsonObj.add("services", serializeServices(src.getServices()));

    return jsonObj;
  }

  @Override
  public ApplicationSpecification deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String name = jsonObj.get("name").getAsString();
    String description = jsonObj.get("description").getAsString();

    Map<String, StreamSpecification> streams = deserializeMap(jsonObj.get("streams"),
                                                              context, StreamSpecification.class);
    Map<String, DataSetSpecification> datasets = deserializeMap(jsonObj.get("datasets"),
                                                                context, DataSetSpecification.class);
    Map<String, String> datasetModules = deserializeMap(jsonObj.get("datasetModules"), context, String.class);
    Map<String, DatasetInstanceCreationSpec> datasetInstances = deserializeMap(jsonObj.get("datasetInstances"),
                                                                               context,
                                                                               DatasetInstanceCreationSpec.class);
    Map<String, FlowSpecification> flows = deserializeMap(jsonObj.get("flows"),
                                                          context, FlowSpecification.class);
    Map<String, ProcedureSpecification> procedures = deserializeMap(jsonObj.get("procedures"),
                                                                    context, ProcedureSpecification.class);
    Map<String, MapReduceSpecification> mapReduces = deserializeMap(jsonObj.get("mapReduces"),
                                                                    context, MapReduceSpecification.class);
    Map<String, WorkflowSpecification> workflows = deserializeMap(jsonObj.get("workflows"),
                                                                  context, WorkflowSpecification.class);

    Map<String, TwillSpecification> services = deseralizeServices(jsonObj.get("services"));

    return new DefaultApplicationSpecification(name, description, streams, datasets,
                                               datasetModules, datasetInstances,
                                               flows, procedures, mapReduces,
                                               workflows, services);
  }

  private Map<String, TwillSpecification> deseralizeServices(JsonElement services) {
    Map<String, TwillSpecification> servicesMap = Maps.newHashMap();
    if (services != null) {
      for (JsonElement element : services.getAsJsonArray()) {
        String spec = element.getAsJsonObject().get("spec").getAsString();
        TwillSpecification twillSpecification = adapter.fromJson(spec);
        servicesMap.put(twillSpecification.getName(), twillSpecification);
      }
    }
    return servicesMap;
  }

  private JsonArray serializeServices(Map<String, TwillSpecification> services) {
    JsonArray array = new JsonArray();
    for (TwillSpecification spec : services.values()) {
      JsonObject object = new JsonObject();
      object.addProperty("spec", adapter.toJson(spec));
      array.add(object);
    }
    return array;
  }
}
