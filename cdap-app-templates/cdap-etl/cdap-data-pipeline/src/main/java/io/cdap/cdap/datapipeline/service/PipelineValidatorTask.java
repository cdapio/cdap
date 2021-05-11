/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.datapipeline.service;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.InvalidPluginProperty;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.task.RunnableTask;
import io.cdap.cdap.api.task.TaskPluginContext;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.common.ArtifactSelectorProvider;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.spec.TrackedPluginSelector;
import io.cdap.cdap.etl.validation.ValidatingConfigurer;
import io.cdap.cdap.proto.id.NamespaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Task for validating a pipeline
 */
public class PipelineValidatorTask extends RunnableTask {

  private TaskPluginContext taskPluginContext;
  private ClassLoader parentClassLoader;
  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(PipelineValidatorTask.class);

  @Inject
  PipelineValidatorTask(TaskPluginContext taskPluginContext) {
    this.taskPluginContext = taskPluginContext;
  }

  @Override
  public void setParentClassLoader(ClassLoader classLoader) {
    this.parentClassLoader = classLoader;
  }

  @Override
  protected byte[] run(String param) throws Exception {
    List<ValidationFailure> validationFailures = new ArrayList<>();
    LOG.info("Receieved param " + param);
    TaskPluginConfig taskPluginConfig = GSON.fromJson(param, TaskPluginConfig.class);
    String pluginName = taskPluginConfig.getPluginName();
    String type = taskPluginConfig.getType();
    ArtifactSelectorConfig artifactSelectorConfig = new ArtifactSelectorConfig(taskPluginConfig.getArtifactScope(),
                                                                               taskPluginConfig.getArtifactName(),
                                                                               taskPluginConfig.getArtifactVersion());
    TrackedPluginSelector pluginSelector = new TrackedPluginSelector(
      new ArtifactSelectorProvider().getPluginSelector(artifactSelectorConfig));
    Object plugin = null;
    try {

      PluginProperties pluginProperties = PluginProperties.builder().addAll(taskPluginConfig.getPluginProperties())
        .build();

      String pluginNamespace = ArtifactScope
        .valueOf(taskPluginConfig.getArtifactScope()) == ArtifactScope.SYSTEM ? NamespaceId.SYSTEM.getNamespace() :
        taskPluginConfig.getNamespace();
      PluginConfigurer pluginConfigurer = taskPluginContext
        .createPluginConfigurer(pluginNamespace, taskPluginConfig.getArtifactName(),
                                taskPluginConfig.getArtifactVersion(), this.parentClassLoader);
      ValidatingConfigurer validatingConfigurer =
        new ValidatingConfigurer(pluginConfigurer);
    /*DefaultStageConfigurer defaultStageConfigurer = new DefaultStageConfigurer(stageName);
    DefaultPipelineConfigurer pipelineConfigurer =
      new DefaultPipelineConfigurer(validatingConfigurer, stageName, Engine.SPARK, defaultStageConfigurer);*/


      //add("pluginProperty", paramMap.get("pluginProperties")).build();

      // Call to usePlugin may throw IllegalArgumentException if hte plugin with the same id is already deployed.
      // This would mean there is a bug in the app and this can not be fixed by user. That is why it is not handled as
      // a ValidationFailure.
      plugin = validatingConfigurer.usePlugin(type, pluginName,
                                              taskPluginConfig.getStageName(), pluginProperties, pluginSelector);
      LOG.error("created plugin instance!!!", plugin.toString());
    } catch (InvalidPluginConfigException e) {
      int numFailures = 0;
      for (String missingProperty : e.getMissingProperties()) {
        String message = String.format("Required property '%s' has no value.", missingProperty);
        validationFailures.add(new ValidationFailure(message, null).withConfigProperty(missingProperty));
        numFailures++;
      }
      for (InvalidPluginProperty invalidProperty : e.getInvalidProperties()) {
        validationFailures
          .add(new ValidationFailure(e.getMessage(), null).withConfigProperty(invalidProperty.getName()));
        numFailures++;
      }
      // if plugin instantiation didn't fail because of a missing property or an invalid property,
      // create a generic failure
      if (numFailures == 0) {
        validationFailures
          .add(new ValidationFailure(e.getMessage(), null));
      }
      if (plugin == null) {
        String errorMessage = String.format("Plugin named '%s' of type '%s' not found.", pluginName, type);
        String correctiveAction = String.format("Make sure plugin '%s' of type '%s' is already deployed.",
                                                pluginName, type);
        ArtifactId requestedArtifactId = artifactSelectorConfig == null ? null :
          new ArtifactId(artifactSelectorConfig.getName(), new ArtifactVersion(artifactSelectorConfig.getVersion()),
                         ArtifactScope.valueOf(artifactSelectorConfig.getScope()));

        ArtifactSelectorConfig suggestion = pluginSelector.getSuggestion();
        ArtifactId suggestedArtifactId = null;
        if (suggestion != null) {
          suggestedArtifactId = new ArtifactId(suggestion.getName(), new ArtifactVersion(suggestion.getVersion()),
                                               ArtifactScope.valueOf(suggestion.getScope()));

        }
        validationFailures.add(new ValidationFailure(e.getMessage(), null)
                                 .withPluginNotFound(taskPluginConfig.getStageName(), pluginName, type,
                                                     requestedArtifactId,
                                                     suggestedArtifactId));
        // throw validation exception if the plugin is not initialized
        //collector.getOrThrowException();
        return errorMessage.getBytes();
      }

    } catch (Exception e) {
      // TODO: Catch specific exceptions when CDAP-15744 is fixed
      //collector.addFailure(e.getMessage(), null).withStacktrace(e.getStackTrace());
      //return e.getMessage().getBytes();
      LOG.error("Got error in PipelineValidatorTask ", e);
      throw e;
    }

    // throw validation exception if any error occurred while creating a new instance of the plugin
    //collector.getOrThrowException();
    return ("created plugin object" + plugin.toString()).getBytes();

  }

  //@Override
  protected void startUp() throws Exception {

  }

  //@Override
  protected void shutDown() throws Exception {

  }

}
