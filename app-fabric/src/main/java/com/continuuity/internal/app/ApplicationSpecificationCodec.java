/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app;

import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.api.service.ServiceSpecification;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.data.dataset.DatasetCreationSpec;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;

import java.lang.reflect.Type;
import java.util.Map;

/**
 *
 */
final class ApplicationSpecificationCodec extends AbstractSpecificationCodec<ApplicationSpecification> {

  @Override
  public JsonElement serialize(ApplicationSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();

    jsonObj.add("name", new JsonPrimitive(src.getName()));
    jsonObj.add("description", new JsonPrimitive(src.getDescription()));
    jsonObj.add("streams", serializeMap(src.getStreams(), context, StreamSpecification.class));
    jsonObj.add("datasets", serializeMap(src.getDataSets(), context, DataSetSpecification.class));
    jsonObj.add("datasetModules", serializeMap(src.getDatasetModules(), context, String.class));
    jsonObj.add("datasetInstances", serializeMap(src.getDatasets(), context, DatasetCreationSpec.class));
    jsonObj.add("flows", serializeMap(src.getFlows(), context, FlowSpecification.class));
    jsonObj.add("procedures", serializeMap(src.getProcedures(), context, ProcedureSpecification.class));
    jsonObj.add("mapReduces", serializeMap(src.getMapReduce(), context, MapReduceSpecification.class));
    jsonObj.add("workflows", serializeMap(src.getWorkflows(), context, WorkflowSpecification.class));
    jsonObj.add("services", serializeMap(src.getServices(), context, ServiceSpecification.class));

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
    Map<String, DatasetCreationSpec> datasetInstances = deserializeMap(jsonObj.get("datasetInstances"),
                                                                               context,
                                                                               DatasetCreationSpec.class);
    Map<String, FlowSpecification> flows = deserializeMap(jsonObj.get("flows"),
                                                          context, FlowSpecification.class);
    Map<String, ProcedureSpecification> procedures = deserializeMap(jsonObj.get("procedures"),
                                                                    context, ProcedureSpecification.class);
    Map<String, MapReduceSpecification> mapReduces = deserializeMap(jsonObj.get("mapReduces"),
                                                                    context, MapReduceSpecification.class);
    Map<String, WorkflowSpecification> workflows = deserializeMap(jsonObj.get("workflows"),
                                                                  context, WorkflowSpecification.class);

    Map<String, ServiceSpecification> services = deserializeMap(jsonObj.get("services"),
                                                                context, ServiceSpecification.class);

    return new DefaultApplicationSpecification(name, description, streams, datasets,
                                               datasetModules, datasetInstances,
                                               flows, procedures, mapReduces,
                                               workflows, services);
  }
}
