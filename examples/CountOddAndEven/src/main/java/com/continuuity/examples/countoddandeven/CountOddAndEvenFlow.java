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
package com.continuuity.examples.countoddandeven;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

/**
 * Count Odd and Even Flow Definition.
 */
public class CountOddAndEvenFlow implements Flow {
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("CountOddAndEven")
      .setDescription("Flow for counting words")
      .withFlowlets()
        .add("NumGenerator", new RandomNumberGenerator())
        .add("OddOrEven", new OddOrEven())
        .add("EvenCounter", new EvenCounter())
        .add("OddCounter", new OddCounter())
      .connect()
        .from("NumGenerator").to("OddOrEven")
        .from("OddOrEven").to("EvenCounter")
        .from("OddOrEven").to("OddCounter")
      .build();
  }
}
