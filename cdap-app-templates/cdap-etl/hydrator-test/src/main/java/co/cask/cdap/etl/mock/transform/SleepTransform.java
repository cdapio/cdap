/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.mock.transform;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.messaging.TopicAlreadyExistsException;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.FieldLevelLineage;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.StageSubmitterContext;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.etl.proto.v2.ETLPlugin;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Sleeps for a configurable amount of time before emitting the input. This is used to test the time spent metric.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("Sleep")
public class SleepTransform extends Transform<StructuredRecord, StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Config config;

  public SleepTransform(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    if (config.millis < 1) {
      throw new IllegalArgumentException("millis must be at least 1.");
    }
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    stageConfigurer.setOutputSchema(stageConfigurer.getInputSchema());
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    try {
      context.createTopic("sleepTopic");
    } catch (TopicAlreadyExistsException e) {
      // ok
    }
    context.getMessagePublisher().publish(context.getNamespace(), "sleepTopic", Long.toString(config.millis));
    FieldLevelLineage o = null;
    // actually record lineage into o
    context.recordLineage(o);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    TimeUnit.MILLISECONDS.sleep(config.millis);
    emitter.emit(input);
  }

  /**
   * Config for plugin
   */
  public static class Config extends PluginConfig {
    @Nullable
    private Long millis;

    public Config() {
      millis = 1L;
    }
  }

  public static ETLPlugin getPlugin(long millis) {
    Map<String, String> properties = new HashMap<>();
    properties.put("millis", Long.toString(millis));
    return new ETLPlugin("Sleep", Transform.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("millis", new PluginPropertyField("millis", "", "long", false, false));
    return new PluginClass(Transform.PLUGIN_TYPE, "Sleep", "", SleepTransform.class.getName(),
                           "config", properties);
  }
}
