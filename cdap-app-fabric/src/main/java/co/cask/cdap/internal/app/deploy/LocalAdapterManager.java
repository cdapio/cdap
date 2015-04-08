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
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.internal.app.deploy.pipeline.adapter.AdapterDeploymentInfo;
import co.cask.cdap.internal.app.deploy.pipeline.adapter.AdapterRegistrationStage;
import co.cask.cdap.internal.app.deploy.pipeline.adapter.AdapterVerificationStage;
import co.cask.cdap.internal.app.deploy.pipeline.adapter.ConfigureAdapterStage;
import co.cask.cdap.internal.app.deploy.pipeline.adapter.CreateAdapterDatasetInstancesStage;
import co.cask.cdap.internal.app.deploy.pipeline.adapter.CreateAdapterStreamsStage;
import co.cask.cdap.internal.app.deploy.pipeline.adapter.DeployAdapterDatasetModulesStage;
import co.cask.cdap.pipeline.Pipeline;
import co.cask.cdap.pipeline.PipelineFactory;
import co.cask.cdap.proto.Id;
import co.cask.cdap.templates.AdapterSpecification;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;

/**
 * This class is concrete implementation of {@link Manager} that deploys an Adapter.
 */
public class LocalAdapterManager implements Manager<AdapterDeploymentInfo, AdapterSpecification> {
  private final CConfiguration configuration;
  private final PipelineFactory pipelineFactory;
  private final StreamAdmin streamAdmin;
  private final ExploreFacade exploreFacade;
  private final boolean exploreEnabled;
  private final DatasetFramework datasetFramework;
  private final DatasetFramework inMemoryDatasetFramework;
  private final Store store;

  @Inject
  public LocalAdapterManager(CConfiguration configuration, PipelineFactory pipelineFactory,
                             DatasetFramework datasetFramework,
                             @Named("datasetMDS") DatasetFramework inMemoryDatasetFramework,
                             StreamAdmin streamAdmin, ExploreFacade exploreFacade,
                             Store store) {
    this.configuration = configuration;
    this.pipelineFactory = pipelineFactory;
    this.datasetFramework = datasetFramework;
    this.inMemoryDatasetFramework = inMemoryDatasetFramework;
    this.streamAdmin = streamAdmin;
    this.exploreFacade = exploreFacade;
    this.store = store;
    this.exploreEnabled = configuration.getBoolean(Constants.Explore.EXPLORE_ENABLED);
  }

  @Override
  public ListenableFuture<AdapterSpecification> deploy(Id.Namespace namespace, String id,
                                                       AdapterDeploymentInfo input) throws Exception {
    Location templateJarLocation =
      new LocalLocationFactory().create(input.getTemplateInfo().getFile().toURI());
    Pipeline<AdapterSpecification> pipeline = pipelineFactory.getPipeline();
    pipeline.addLast(new ConfigureAdapterStage(namespace, id, templateJarLocation));
    pipeline.addLast(new AdapterVerificationStage(input.getTemplateSpec()));
    pipeline.addLast(new DeployAdapterDatasetModulesStage(configuration, namespace, templateJarLocation,
                                                          datasetFramework, inMemoryDatasetFramework));
    pipeline.addLast(new CreateAdapterDatasetInstancesStage(configuration, datasetFramework, namespace));
    pipeline.addLast(new CreateAdapterStreamsStage(namespace, streamAdmin, exploreFacade, exploreEnabled));
    pipeline.setFinally(new AdapterRegistrationStage(namespace, store));
    return pipeline.execute(input);
  }
}
