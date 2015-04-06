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
import co.cask.cdap.internal.app.deploy.pipeline.AdapterDeploymentInfo;
import co.cask.cdap.internal.app.deploy.pipeline.AdapterRegistrationStage;
import co.cask.cdap.internal.app.deploy.pipeline.AdapterVerificationStage;
import co.cask.cdap.internal.app.deploy.pipeline.ConfigureAdapterStage;
import co.cask.cdap.pipeline.Pipeline;
import co.cask.cdap.pipeline.PipelineFactory;
import co.cask.cdap.proto.Id;
import co.cask.cdap.templates.AdapterSpecification;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;

import javax.annotation.Nullable;

/**
 * This class is concrete implementation of {@link Manager} that deploys an Adapter.
 */
public class LocalAdapterManager implements Manager<AdapterDeploymentInfo, AdapterSpecification> {
  private final PipelineFactory pipelineFactory;
  private final Store store;

  @Inject
  public LocalAdapterManager(PipelineFactory pipelineFactory,
                             Store store) {
    this.pipelineFactory = pipelineFactory;
    this.store = store;
  }

  @Override
  public ListenableFuture<AdapterSpecification> deploy(Id.Namespace namespace, String id,
                                                       AdapterDeploymentInfo input) throws Exception {
    Pipeline<AdapterSpecification> pipeline = pipelineFactory.getPipeline();
    pipeline.addLast(new ConfigureAdapterStage(namespace, id));
    pipeline.addLast(new AdapterVerificationStage(input.getTemplateSpec()));
    // TODO: stages for creating dataset instances and streams
    pipeline.setFinally(new AdapterRegistrationStage(namespace, store));
    return pipeline.execute(input);
  }
}
