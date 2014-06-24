/**
 * Copyright 2013-2014 Continuuity, Inc.
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
package com.continuuity.examples.resourcespammer;

import com.continuuity.api.Resources;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.RoundRobin;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.lib.KeyValueTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;

/**
 * Flowlet designed to use lots of CPU resources {@code Spammer1Core}.
 */
public class Spammer2Core extends AbstractFlowlet {
  private final Spammer spammer;

  @UseDataSet("output")
  private KeyValueTable output;

  @UseDataSet("input")
  private KeyValueTable input;

  @Override
  public FlowletSpecification configure() {
    return FlowletSpecification.Builder.with()
      .setName("2CoreSpammer")
      .setDescription("spams with 2 cores")
      .withResources(new Resources(512, 2))
      .build();
  }

  public Spammer2Core() {
    spammer = new Spammer(2);
  }

  @RoundRobin
  @ProcessInput("out")
  public void process(Integer number) {
    long duration = spammer.spamFor(2 * 1000 * 1000);
    System.out.println("spammer spun for " + duration + " ms");
    output.write(Bytes.toBytes(1), Bytes.toBytes(1));
    input.write(Bytes.toBytes(1), Bytes.toBytes(1));
  }
}
