/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.runtime.app;

import com.continuuity.api.Application;
import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.Tick;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class MultiApp implements Application {

  public static final byte[] KEY = new byte[] {'k', 'e', 'y'};

  @Override
  public com.continuuity.api.ApplicationSpecification configure() {
    return com.continuuity.api.ApplicationSpecification.Builder.with()
      .setName("MultiApp")
      .setDescription("MultiApp")
      .noStream()
      .withDataSets().add(new KeyValueTable("accumulated"))
      .withFlows().add(new MultiFlow())
      .noProcedure()
      .noMapReduce()
      .noWorkflow()
      .build();
  }

  /**
   *
   */
  public static final class MultiFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("MultiFlow")
        .setDescription("MultiFlow")
        .withFlowlets()
          .add("gen", new Generator())
          .add("c1", new Consumer(), 2)
          .add("c2", new Consumer(), 2)
          .add("c3", new ConsumerStr(), 2)
        .connect()
          .from("gen").to("c1")
          .from("gen").to("c2")
          .from("gen").to("c3")
        .build();
    }
  }

  /**
   *
   */
  public static final class Generator extends AbstractFlowlet {

    private OutputEmitter<Integer> output;
    @Output("str")
    private OutputEmitter<String> outString;
    private int i;

    @Tick(delay = 1L, unit = TimeUnit.NANOSECONDS)
    public void generate() throws Exception {
      if (i < 100) {
        output.emit(i);
        outString.emit(Integer.toString(i));
        i++;
      }
    }
  }

  /**
   *
   */
  public static final class Consumer extends AbstractFlowlet {
    @UseDataSet("accumulated")
    private KeyValueTable accumulated;

    @ProcessInput(maxRetries = Integer.MAX_VALUE)
    public void process(long l) {
      accumulated.increment(KEY, l);
    }
  }

  /**
   *
   */
  public static final class ConsumerStr extends AbstractFlowlet {
    @UseDataSet("accumulated")
    private KeyValueTable accumulated;

    @ProcessInput(value = "str", maxRetries = Integer.MAX_VALUE)
    public void process(String str) {
      accumulated.increment(KEY, Long.valueOf(str));
    }
  }
}
