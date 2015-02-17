/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.Id;
import com.google.common.reflect.TypeToken;

/**
 * This {@link co.cask.cdap.pipeline.Stage} is responsible for automatic creation of any new streams specified by the
 * application. Additionally, it will enable exploration of those streams if exploration is enabled.
 */
public class CreateStreamsStage extends AbstractStage<ApplicationDeployable> {
  private final Id.Namespace namespace;
  private final StreamAdmin streamAdmin;
  private final ExploreFacade exploreFacade;
  private final boolean enableExplore;

  public CreateStreamsStage(Id.Namespace namespace, StreamAdmin streamAdmin, ExploreFacade exploreFacade,
                            boolean enableExplore) {
    super(TypeToken.of(ApplicationDeployable.class));
    this.namespace = namespace;
    this.streamAdmin = streamAdmin;
    this.exploreFacade = exploreFacade;
    this.enableExplore = enableExplore;
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
    for (String streamName : specification.getStreams().keySet()) {
      Id.Stream streamId = Id.Stream.from(namespace, streamName);
      // create the stream and enable exploration if the stream doesn't already exist.
      if (!streamAdmin.exists(streamId)) {
        streamAdmin.create(streamId);
        if (enableExplore) {
          exploreFacade.enableExploreStream(streamName);
        }
      }
    }

    // Emit the input to next stage.
    emit(input);
  }
}
