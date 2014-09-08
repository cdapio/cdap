/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.reactor.client.app;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.InputContext;
import co.cask.cdap.api.flow.flowlet.StreamEvent;

import java.nio.charset.CharacterCodingException;

/**
 *
 */
public class FakeFlow implements Flow {

  public static final String NAME = "FakeFlow";
  public static final String FLOWLET_NAME = "fakeFlowlet";

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName(NAME)
      .setDescription("Does nothing")
      .withFlowlets()
        .add(FLOWLET_NAME, new FakeFlowlet())
      .connect().fromStream(FakeApp.STREAM_NAME).to(FLOWLET_NAME)
      .build();
  }

  public static final class FakeFlowlet extends AbstractFlowlet {

    @ProcessInput
    public void process(StreamEvent event, InputContext context) throws CharacterCodingException {
      // NO-OP
    }

  }
}
