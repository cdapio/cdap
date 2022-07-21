/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.etl.mock.action;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.lineage.field.OperationType;
import io.cdap.cdap.api.lineage.field.ReadOperation;
import io.cdap.cdap.api.lineage.field.TransformOperation;
import io.cdap.cdap.api.lineage.field.WriteOperation;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.cdap.etl.api.lineage.AccessType;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Mock action to write dummy field lineage information for two datasets
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("FieldLineageAction")
public class FieldLineageAction extends Action {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Operation.class, new OperationTypeAdapter()).create();
  private static final Type LIST_OPERTAION = new TypeToken<List<Operation>>() { }.getType();
  private final Config config;

  /**
   * Config for the FieldLineageAction
   */
  public static class Config extends PluginConfig {
    private String readDataset;
    private String writeDataset;
    // json string for the field operations
    private String fieldOperations;
  }

  public FieldLineageAction(Config config) {
    this.config = config;
  }

  @Override
  public void run(ActionContext context) throws Exception {
    context.registerLineage(config.readDataset, AccessType.READ);
    context.registerLineage(config.writeDataset, AccessType.WRITE);

    List<Operation> operations = GSON.fromJson(config.fieldOperations, LIST_OPERTAION);
    context.record(operations);
  }

  public static ETLPlugin getPlugin(String readDataset, String writeDataset, Collection<Operation> fieldOperations) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("readDataset", readDataset);
    builder.put("writeDataset", writeDataset);
    builder.put("fieldOperations", GSON.toJson(fieldOperations));
    return new ETLPlugin("FieldLineageAction", Action.PLUGIN_TYPE, builder.build(), null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("readDataset", new PluginPropertyField("readDataset", "", "string", true, false));
    properties.put("writeDataset", new PluginPropertyField("writeDataset", "", "string", true, false));
    properties.put("fieldOperations", new PluginPropertyField("fieldOperations", "", "string", true, false));

    return PluginClass.builder().setName("FieldLineageAction").setType(Action.PLUGIN_TYPE)
             .setDescription("").setClassName(FieldLineageAction.class.getName()).setProperties(properties)
             .setConfigFieldName("config").build();
  }

  /**
   * Type adapter for {@link Operation}.
   */
  private static class OperationTypeAdapter implements JsonSerializer<Operation>, JsonDeserializer<Operation> {
    @Override
    public JsonElement serialize(Operation src, Type typeOfSrc, JsonSerializationContext context) {
      return context.serialize(src);
    }

    @Override
    public Operation deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
      throws JsonParseException {
      JsonObject jsonObj = json.getAsJsonObject();

      OperationType type = context.deserialize(jsonObj.get("type"), OperationType.class);

      switch (type) {
        case READ:
          return context.deserialize(json, ReadOperation.class);
        case TRANSFORM:
          return context.deserialize(json, TransformOperation.class);
        case WRITE:
          return context.deserialize(json, WriteOperation.class);
      }
      return null;
    }
  }
}
