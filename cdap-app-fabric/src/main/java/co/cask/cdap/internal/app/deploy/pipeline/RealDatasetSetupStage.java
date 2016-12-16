/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.data2.dataset2.preview.PreviewDatasetFramework;
import co.cask.cdap.pipeline.AbstractStage;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;

import java.util.Set;

/**
 * This {@link co.cask.cdap.pipeline.Stage} is responsible for setting up the datasets in real space for preview
 */
public class RealDatasetSetupStage extends AbstractStage<ApplicationDeployable> {
  private final PreviewDatasetFramework previewDatasetFramework;

  public RealDatasetSetupStage(PreviewDatasetFramework previewDatasetFramework) {
    super(TypeToken.of(ApplicationDeployable.class));
    this.previewDatasetFramework = previewDatasetFramework;
  }

  /**
   * Get the datasets from {@link ApplicationSpecification} and set it for {@link PreviewDatasetFramework}
   */
  @Override
  public void process(ApplicationDeployable input) throws Exception {
    ApplicationSpecification applicationSpec = input.getSpecification();
    previewDatasetFramework.addRealDatasets(applicationSpec.getDatasets().keySet());
    emit(input);
  }
}
