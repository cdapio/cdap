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

package co.cask.cdap.internal.app.deploy;

import co.cask.cdap.app.deploy.Manager;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.internal.app.deploy.pipeline.adapter.AdapterDeploymentInfo;
import co.cask.cdap.internal.app.deploy.pipeline.adapter.AdapterRegistrationStage;
import co.cask.cdap.internal.app.deploy.pipeline.adapter.AdapterVerificationStage;
import co.cask.cdap.internal.app.deploy.pipeline.adapter.ConfigureAdapterStage;
import co.cask.cdap.internal.app.deploy.pipeline.adapter.CreateAdapterDatasetInstancesStage;
import co.cask.cdap.internal.app.deploy.pipeline.adapter.CreateAdapterStreamsStage;
import co.cask.cdap.internal.app.deploy.pipeline.adapter.DeployAdapterDatasetModulesStage;
import co.cask.cdap.internal.app.runtime.adapter.PluginRepository;
import co.cask.cdap.pipeline.Pipeline;
import co.cask.cdap.pipeline.PipelineFactory;
import co.cask.cdap.proto.Id;
import co.cask.cdap.templates.AdapterDefinition;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;

/**
 * This class is concrete implementation of {@link Manager} that deploys an Adapter.
 */
public class LocalAdapterManager implements Manager<AdapterDeploymentInfo, AdapterDefinition> {
  private final CConfiguration configuration;
  private final PipelineFactory pipelineFactory;
  private final StreamAdmin streamAdmin;
  private final ExploreFacade exploreFacade;
  private final boolean exploreEnabled;
  private final DatasetFramework datasetFramework;
  private final DatasetFramework inMemoryDatasetFramework;
  private final Store store;
  private final PluginRepository pluginRepository;
  private final UsageRegistry usageRegistry;

  @Inject
  public LocalAdapterManager(CConfiguration configuration, PipelineFactory pipelineFactory,
                             DatasetFramework datasetFramework,
                             @Named("datasetMDS") DatasetFramework inMemoryDatasetFramework,
                             StreamAdmin streamAdmin, ExploreFacade exploreFacade,
                             Store store, PluginRepository pluginRepository,
                             UsageRegistry usageRegistry) {
    this.configuration = configuration;
    this.pipelineFactory = pipelineFactory;
    this.datasetFramework = datasetFramework;
    this.inMemoryDatasetFramework = inMemoryDatasetFramework;
    this.streamAdmin = streamAdmin;
    this.exploreFacade = exploreFacade;
    this.store = store;
    this.pluginRepository = pluginRepository;
    this.exploreEnabled = configuration.getBoolean(Constants.Explore.EXPLORE_ENABLED);
    this.usageRegistry = usageRegistry;
  }

  @Override
  public ListenableFuture<AdapterDefinition> deploy(Id.Namespace namespace, String id,
                                                       AdapterDeploymentInfo input) throws Exception {
    Location templateJarLocation =
      new LocalLocationFactory().create(input.getTemplateInfo().getFile().toURI());
    Pipeline<AdapterDefinition> pipeline = pipelineFactory.getPipeline();
    pipeline.addLast(new ConfigureAdapterStage(configuration, namespace, id, templateJarLocation, pluginRepository));
    pipeline.addLast(new AdapterVerificationStage(input.getTemplateSpec()));
    pipeline.addLast(new DeployAdapterDatasetModulesStage(configuration, namespace, templateJarLocation,
                                                          datasetFramework, inMemoryDatasetFramework));
    pipeline.addLast(new CreateAdapterDatasetInstancesStage(configuration, datasetFramework, namespace));
    pipeline.addLast(new CreateAdapterStreamsStage(namespace, streamAdmin, exploreFacade, exploreEnabled));
    pipeline.addLast(new AdapterRegistrationStage(namespace, store, usageRegistry));
    return pipeline.execute(input);
  }
}
