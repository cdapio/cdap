/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.internal.app;

import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.internal.workflow.DefaultWorkflowSpecification;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 *
 */
final class WorkflowSpecificationCodec extends AbstractSpecificationCodec<WorkflowSpecification> {

  @Override
  public JsonElement serialize(WorkflowSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();

    jsonObj.add("className", new JsonPrimitive(src.getClassName()));
    jsonObj.add("name", new JsonPrimitive(src.getName()));
    jsonObj.add("description", new JsonPrimitive(src.getDescription()));
    jsonObj.add("actions", serializeList(src.getActions(), context, WorkflowActionSpecification.class));
    jsonObj.add("mapReduces", serializeMap(src.getMapReduce(), context, MapReduceSpecification.class));
    jsonObj.add("sparks", serializeMap(src.getSparks(), context, SparkSpecification.class));
    jsonObj.add("schedules", serializeList(src.getSchedules(), context, Schedule.class));


    return jsonObj;
  }

  @Override
  public WorkflowSpecification deserialize(JsonElement json, Type typeOfT,
                                           JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String className = jsonObj.get("className").getAsString();
    String name = jsonObj.get("name").getAsString();
    String description = jsonObj.get("description").getAsString();
    List<WorkflowActionSpecification> actions = deserializeList(jsonObj.get("actions"), context,
                                                                WorkflowActionSpecification.class);
    Map<String, MapReduceSpecification> mapReduces = deserializeMap(jsonObj.get("mapReduces"), context,
                                                                    MapReduceSpecification.class);

    Map<String, SparkSpecification> sparks = deserializeMap(jsonObj.get("sparks"), context, SparkSpecification.class);

    List<Schedule> schedules = deserializeList(jsonObj.get("schedules"), context, Schedule.class);

    return new DefaultWorkflowSpecification(className, name, description, actions, mapReduces, sparks, schedules);
  }
}
