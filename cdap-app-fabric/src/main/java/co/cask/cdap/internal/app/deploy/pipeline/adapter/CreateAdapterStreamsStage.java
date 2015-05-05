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

import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.internal.app.deploy.pipeline.StreamCreator;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.templates.AdapterDefinition;
import com.google.common.reflect.TypeToken;

/**
 * This {@link co.cask.cdap.pipeline.Stage} is responsible for automatic creation of any new streams specified by the
 * application. Additionally, it will enable exploration of those streams if exploration is enabled.
 */
public class CreateAdapterStreamsStage extends AbstractStage<AdapterDefinition> {
  private final StreamCreator streamCreator;

  public CreateAdapterStreamsStage(Id.Namespace namespace, StreamAdmin streamAdmin, ExploreFacade exploreFacade,
                                   boolean enableExplore) {
    super(TypeToken.of(AdapterDefinition.class));
    this.streamCreator = new StreamCreator(namespace, streamAdmin, exploreFacade, enableExplore);
  }

  /**
   * Create any streams in the given specification.
   *
   * @param input An instance of {@link AdapterDefinition}
   */
  @Override
  public void process(AdapterDefinition input) throws Exception {
    streamCreator.createStreams(input.getStreams().keySet());

    // Emit the input to next stage.
    emit(input);
  }
}
