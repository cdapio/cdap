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

package co.cask.cdap.internal.app.deploy.pipeline.adapter;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.deploy.pipeline.DatasetInstanceCreator;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.templates.AdapterDefinition;
import com.google.common.reflect.TypeToken;

/**
 * This {@link co.cask.cdap.pipeline.Stage} is responsible for automatic
 * deploy of the {@link co.cask.cdap.api.dataset.module.DatasetModule}s specified by application.
 */
public class CreateAdapterDatasetInstancesStage extends AbstractStage<AdapterDefinition> {
  private final DatasetInstanceCreator datasetInstanceCreator;

  public CreateAdapterDatasetInstancesStage(CConfiguration configuration, DatasetFramework datasetFramework,
                                            Id.Namespace namespace) {
    super(TypeToken.of(AdapterDefinition.class));
    this.datasetInstanceCreator = new DatasetInstanceCreator(configuration, datasetFramework, namespace);
  }

  /**
   * Creates the dataset instances contained in the give adapter specification.
   *
   * @param input An instance of {@link AdapterDefinition}
   */
  @Override
  public void process(AdapterDefinition input) throws Exception {
    datasetInstanceCreator.createInstances(input.getDatasets());

    // Emit the input to next stage.
    emit(input);
  }
}
