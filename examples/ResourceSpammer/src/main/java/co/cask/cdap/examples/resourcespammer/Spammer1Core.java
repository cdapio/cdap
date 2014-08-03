/*
 * Copyright 2014 Cask, Inc.
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
package co.cask.cdap.examples.resourcespammer;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.RoundRobin;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletSpecification;

/**
 * Flowlet designed to use lots of CPU resources {@code Spammer1Core}.
 */
public class Spammer1Core extends AbstractFlowlet {
  private final Spammer spammer;

  @UseDataSet("output")
  private KeyValueTable output;

  @UseDataSet("input")
  private KeyValueTable input;

  @Override
  public FlowletSpecification configure() {
    return FlowletSpecification.Builder.with()
      .setName("1CoreSpammer")
      .setDescription("spams with 1 core")
      .withResources(new Resources(512, 1))
      .build();
  }

  public Spammer1Core() {
    spammer = new Spammer(1);
  }

  @RoundRobin
  @ProcessInput("out")
  public void process(Integer number) {
    long duration = spammer.spamFor(4 * 1000 * 1000);
    System.out.println("spammer spun for " + duration + " ms");
    output.write(Bytes.toBytes(1), Bytes.toBytes(1));
    input.write(Bytes.toBytes(1), Bytes.toBytes(1));
  }
}
