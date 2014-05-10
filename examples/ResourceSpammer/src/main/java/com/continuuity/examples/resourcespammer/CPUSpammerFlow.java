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


import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

/**
 * Resource spammer Flow declaration {@code CPUSpammerFlow}.
 *
 * On a one node cluster with 8 GB memory and 16 virtual cores, everything should
 * be able to run at once and CPU usage can be monitored to verify that limits are being honored.
 * Virtual cores only take effect on hadoop-2.1.0 and later, and require cgroup support from the
 * OS and also require that Hadoop be configured to make use of cgroups with the LinuxContainerExecutor.
 */
public class CPUSpammerFlow implements Flow {

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("CPUSpammerFlow")
      .setDescription("ResourceSpammerApp Flow")
      .withFlowlets()
        .add("generator", new DataGenerator())
        .add("1CoreA", new Spammer1Core())
        .add("1CoreB", new Spammer1Core())
        .add("2CoreA", new Spammer2Core())
        .add("2CoreB", new Spammer2Core())
        .add("4CoreA", new Spammer4Core())
        .add("4CoreB", new Spammer4Core())
      .connect()
        .from("generator").to("1CoreA")
        .from("generator").to("1CoreB")
        .from("generator").to("2CoreA")
        .from("generator").to("2CoreB")
        .from("generator").to("4CoreA")
        .from("generator").to("4CoreB")
      .build();
  }
}
