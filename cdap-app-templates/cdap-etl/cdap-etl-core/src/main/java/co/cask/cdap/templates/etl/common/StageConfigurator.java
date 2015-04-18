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

package co.cask.cdap.templates.etl.common;

import co.cask.cdap.api.templates.AdapterConfigurer;
import co.cask.cdap.templates.etl.api.EndPointStage;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.StageSpecification;
import co.cask.cdap.templates.etl.api.Transform;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

import java.util.List;

/**
 * Configurator utility class that has helper methods to configure Source/Sink and Transform stages.
 */
public class StageConfigurator {
  private static final Gson GSON = new Gson();

  public static void configure(EndPointStage stage, ETLStage stageConfig, AdapterConfigurer configurer,
                               PipelineConfigurer pipelineConfigurer, String specKey) throws Exception {
    stage.configurePipeline(stageConfig, pipelineConfigurer);
    DefaultStageConfigurer defaultStageConfigurer = new DefaultStageConfigurer(stage.getClass());
    StageSpecification specification = defaultStageConfigurer.createSpecification();
    configurer.addRuntimeArgument(specKey, GSON.toJson(specification));
  }

  public static void configureTransforms(List<Transform> transformList, AdapterConfigurer configurer, String specKey) {
    List<StageSpecification> transformSpecs = Lists.newArrayList();
    for (Transform transformObj : transformList) {
      DefaultStageConfigurer stageConfigurer = new DefaultStageConfigurer(transformObj.getClass());
      StageSpecification specification = stageConfigurer.createSpecification();
      transformSpecs.add(specification);
    }
    configurer.addRuntimeArgument(specKey, GSON.toJson(transformSpecs));
  }
}
