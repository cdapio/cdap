/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.pipeline.AbstractStage;
import com.google.common.reflect.TypeToken;

/**
 * This {@link co.cask.cdap.pipeline.Stage} is responsible for automatic creation of any new streams specified by the
 * application. Additionally, it will enable exploration of those streams if exploration is enabled.
 */
public class CreateStreamsStage extends AbstractStage<ApplicationDeployable> {
  private final StreamCreator streamCreator;

  public CreateStreamsStage(StreamAdmin streamAdmin) {
    super(TypeToken.of(ApplicationDeployable.class));
    this.streamCreator = new StreamCreator(streamAdmin);
  }

  /**
   * Receives an input containing application specification and location
   * and verifies both.
   *
   * @param input An instance of {@link ApplicationDeployable}
   */
  @Override
  public void process(ApplicationDeployable input) throws Exception {
    // create stream instances
    ApplicationSpecification specification = input.getSpecification();
    streamCreator.createStreams(input.getApplicationId().getParent(), specification.getStreams().values(),
                                input.getOwnerPrincipal());

    // Emit the input to next stage.
    emit(input);
  }
}
