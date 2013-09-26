/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
