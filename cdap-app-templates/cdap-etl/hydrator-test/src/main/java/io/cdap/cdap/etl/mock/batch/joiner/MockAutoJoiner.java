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
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.batch.BatchAutoJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.join.AutoJoinerContext;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinField;
import io.cdap.cdap.etl.api.join.JoinKey;
import io.cdap.cdap.etl.api.join.JoinStage;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.internal.guava.reflect.TypeToken;

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
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private static final Type LIST = new TypeToken<List<String>>() { }.getType();
  private static final Gson GSON = new Gson();
  private final Conf conf;

  @SuppressWarnings("unused")
  public MockAutoJoiner(Conf conf) {
    this.conf = conf;
  }

  @Nullable
  @Override
  public JoinDefinition define(AutoJoinerContext context) {
    Map<String, JoinStage> inputStages = context.getInputStages();
    List<JoinStage> from = new ArrayList<>(inputStages.size());
    Set<String> required = new HashSet<>(conf.getRequired());
    Set<String> broadcast = new HashSet<>(conf.getBroadcast());
    List<JoinField> selectedFields = new ArrayList<>();
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

      for (Schema.Field field : stage.getSchema().getFields()) {
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
    return builder.build();
  }

  /**
   * Auto Join config.
   */
  @SuppressWarnings("unused")
  public static class Conf extends PluginConfig {
    private String stages;

    private String key;

    @Nullable
    private String required;

    @Nullable
    private Boolean nullSafe;

    private String broadcast;

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
  }

  public static ETLPlugin getPlugin(List<String> stages, List<String> key, List<String> required) {
    return getPlugin(stages, key, required, Collections.emptyList());
  }

  public static ETLPlugin getPlugin(List<String> stages, List<String> key, List<String> required,
                                    List<String> broadcast) {
    Map<String, String> properties = new HashMap<>();
    properties.put("stages", GSON.toJson(stages));
    properties.put("required", GSON.toJson(required));
    properties.put("key", GSON.toJson(key));
    properties.put("broadcast", GSON.toJson(broadcast));
    return new ETLPlugin(NAME, BatchJoiner.PLUGIN_TYPE, properties, null);
  }

  public static ETLPlugin getPlugin(List<String> stages, List<String> key, List<String> required,
                                    boolean nullSafe) {
    Map<String, String> properties = new HashMap<>();
    properties.put("stages", GSON.toJson(stages));
    properties.put("required", GSON.toJson(required));
    properties.put("key", GSON.toJson(key));
    properties.put("nullSafe", Boolean.toString(nullSafe));
    return new ETLPlugin(NAME, BatchJoiner.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("stages", new PluginPropertyField("stages", "", "string", true, false));
    properties.put("required", new PluginPropertyField("required", "", "string", false, false));
    properties.put("key", new PluginPropertyField("key", "", "string", true, false));
    properties.put("nullSafe", new PluginPropertyField("nullSafe", "", "boolean", false, false));
    properties.put("broadcast", new PluginPropertyField("broadcast", "", "string", false, false));
    return new PluginClass(BatchJoiner.PLUGIN_TYPE, NAME, "", MockAutoJoiner.class.getName(), "conf", properties);
  }

}
