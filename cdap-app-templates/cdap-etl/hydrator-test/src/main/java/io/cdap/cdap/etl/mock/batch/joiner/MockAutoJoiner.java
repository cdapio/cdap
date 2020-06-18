/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.mock.batch.joiner;

import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.batch.BatchAutoJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoinerContext;
import io.cdap.cdap.etl.api.join.AutoJoinerContext;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinField;
import io.cdap.cdap.etl.api.join.JoinKey;
import io.cdap.cdap.etl.api.join.JoinStage;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.internal.guava.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Mock auto-joiner. Performs inner, leftouter, or outer joins. Assumes join keys are named the same on both the
 * left and right sides.
 */
@Plugin(type = BatchJoiner.PLUGIN_TYPE)
@Name(MockAutoJoiner.NAME)
public class MockAutoJoiner extends BatchAutoJoiner {
  public static final String NAME = "MockAutoJoiner";
  public static final String PARTITIONS_ARGUMENT = "partitions";
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private static final Gson GSON = new Gson();
  private static final Type LIST = new TypeToken<List<String>>() { }.getType();
  private static final Type SELECT_TYPE = new TypeToken<List<JoinField>>() { }.getType();
  private final Conf conf;

  @SuppressWarnings("unused")
  public MockAutoJoiner(Conf conf) {
    this.conf = conf;
  }

  @Nullable
  @Override
  public JoinDefinition define(AutoJoinerContext context) {
    if (conf.containsMacro(Conf.STAGES) || conf.containsMacro(Conf.KEY) || conf.containsMacro(Conf.REQUIRED) ||
      conf.containsMacro(Conf.SELECT)) {
      return null;
    }

    Map<String, JoinStage> inputStages = context.getInputStages();
    List<JoinStage> from = new ArrayList<>(inputStages.size());
    Set<String> required = new HashSet<>(conf.getRequired());
    Set<String> broadcast = new HashSet<>(conf.getBroadcast());
    List<JoinField> selectedFields = conf.getSelect();
    boolean shouldGenerateSelected = selectedFields.isEmpty();
    JoinCondition.OnKeys.Builder condition = JoinCondition.onKeys()
      .setNullSafe(conf.isNullSafe());
    for (String stageName : conf.getStages()) {
      JoinStage.Builder stageBuilder = JoinStage.builder(inputStages.get(stageName));
      if (!required.contains(stageName)) {
        stageBuilder.isOptional();
      }
      if (broadcast.contains(stageName)) {
        stageBuilder.setBroadcast(true);
      }
      JoinStage stage = stageBuilder.build();
      from.add(stage);

      condition.addKey(new JoinKey(stageName, conf.getKey()));

      Schema stageSchema = stage.getSchema();
      if (!shouldGenerateSelected || stageSchema == null) {
        continue;
      }

      for (Schema.Field field : stageSchema.getFields()) {
        // alias everything to stage_field
        selectedFields.add(new JoinField(stageName, field.getName(),
                                         String.format("%s_%s", stageName, field.getName())));
      }
    }

    JoinDefinition.Builder builder = JoinDefinition.builder()
      .select(selectedFields)
      .on(condition.build())
      .from(from)
      .setOutputSchemaName(String.join(".", conf.getStages()));
    Schema outputSchema = conf.getSchema();
    if (outputSchema != null) {
      builder.setOutputSchema(outputSchema);
    }
    return builder.build();
  }

  @Override
  public void prepareRun(BatchJoinerContext context) throws Exception {
    super.prepareRun(context);
    String partitionsStr = context.getArguments().get(PARTITIONS_ARGUMENT);
    if (partitionsStr != null) {
      context.setNumPartitions(Integer.parseInt(partitionsStr));
    }
  }

  /**
   * Auto Join config.
   */
  @SuppressWarnings("unused")
  public static class Conf extends PluginConfig {
    public static final String STAGES = "stages";
    public static final String KEY = "key";
    public static final String REQUIRED = "required";
    public static final String NULL_SAFE = "nullSafe";
    public static final String SELECT = "select";
    public static final String BROADCAST = "broadcast";
    public static final String SCHEMA = "schema";

    @Macro
    private String stages;

    @Macro
    private String key;

    @Macro
    @Nullable
    private String required;

    @Nullable
    private Boolean nullSafe;

    private String broadcast;

    @Macro
    @Nullable
    private String select;

    @Macro
    @Nullable
    private String schema;

    List<String> getKey() {
      return GSON.fromJson(key, LIST);
    }

    List<String> getStages() {
      return GSON.fromJson(stages, LIST);
    }

    List<String> getRequired() {
      return required == null ? Collections.emptyList() : GSON.fromJson(required, LIST);
    }

    boolean isNullSafe() {
      return nullSafe == null ? true : nullSafe;
    }

    List<String> getBroadcast() {
      return broadcast == null ? Collections.emptyList() : GSON.fromJson(broadcast, LIST);
    }

    List<JoinField> getSelect() {
      return select == null ? Collections.emptyList() : GSON.fromJson(select, SELECT_TYPE);
    }

    @Nullable
    Schema getSchema() {
      try {
        return schema == null ? null : Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  public static ETLPlugin getPlugin(List<String> stages, List<String> key, List<String> required,
                                    List<String> broadcast, List<JoinField> select, boolean nullSafe) {
    return new ETLPlugin(NAME, BatchAutoJoiner.PLUGIN_TYPE,
                         getProperties(stages, key, required, broadcast, select, nullSafe));
  }

  public static Map<String, String> getProperties(List<String> stages, List<String> key, List<String> required,
                                                  List<String> broadcast, List<JoinField> select, boolean nullSafe) {
    Map<String, String> properties = new HashMap<>();
    properties.put(Conf.STAGES, GSON.toJson(stages));
    properties.put(Conf.REQUIRED, GSON.toJson(required));
    properties.put(Conf.KEY, GSON.toJson(key));
    properties.put(Conf.BROADCAST, GSON.toJson(broadcast));
    properties.put(Conf.SELECT, GSON.toJson(select));
    properties.put(Conf.NULL_SAFE, Boolean.toString(nullSafe));
    return properties;
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put(Conf.STAGES, new PluginPropertyField(Conf.STAGES, "", "string", true, true));
    properties.put(Conf.REQUIRED, new PluginPropertyField(Conf.REQUIRED, "", "string", false, true));
    properties.put(Conf.KEY, new PluginPropertyField(Conf.KEY, "", "string", true, true));
    properties.put(Conf.NULL_SAFE, new PluginPropertyField(Conf.NULL_SAFE, "", "boolean", false, false));
    properties.put(Conf.BROADCAST, new PluginPropertyField(Conf.BROADCAST, "", "string", false, false));
    properties.put(Conf.SELECT, new PluginPropertyField(Conf.SELECT, "", "string", false, true));
    properties.put(Conf.SCHEMA, new PluginPropertyField(Conf.SCHEMA, "", "string", false, true));
    return new PluginClass(BatchJoiner.PLUGIN_TYPE, NAME, "", MockAutoJoiner.class.getName(), "conf", properties);
  }

}
