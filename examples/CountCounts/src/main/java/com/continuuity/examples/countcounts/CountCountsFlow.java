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
package com.continuuity.examples.countcounts;


import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

/**
 * Count Counts Flow.
 */
public class CountCountsFlow implements Flow {

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("CountCounts")
      .setDescription("Flow for counting words")
      .withFlowlets()
        .add("source", new StreamSource())
        .add("counter", new WordCounter())
        .add("incrementer", new Incrementer())
      .connect()
        .fromStream("text").to("source")
        .from("source").to("counter")
        .from("counter").to("incrementer")
      .build();
  }
}
