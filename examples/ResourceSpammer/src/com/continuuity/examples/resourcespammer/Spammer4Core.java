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

import com.continuuity.api.Resources;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.RoundRobin;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;

/**
 * Flowlet designed to use lots of CPU resources {@code Spammer1Core}.
 */
public class Spammer4Core extends AbstractFlowlet {
  private final Spammer spammer;

  @UseDataSet("output")
  KeyValueTable output;

  @UseDataSet("input")
  KeyValueTable input;

  @Override
  public FlowletSpecification configure() {
    return FlowletSpecification.Builder.with()
      .setName("4CoreSpammer")
      .setDescription("spams with 4 cores")
      .withResources(new Resources(512, 4))
      .build();
  }

  public Spammer4Core() {
    spammer = new Spammer(4);
  }

  @RoundRobin
  @ProcessInput("out")
  public void process(Integer number) {
    long duration = spammer.spamFor(1000 * 1000);
    System.out.println("spammer spun for " + duration + " ms");
    output.write(Bytes.toBytes(1), Bytes.toBytes(1));
    input.write(Bytes.toBytes(1), Bytes.toBytes(1));
  }
}
