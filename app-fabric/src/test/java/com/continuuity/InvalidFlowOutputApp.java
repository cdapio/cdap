/*
 * Copyright 2012-2014 Continuuity, Inc.
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
package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Tick;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class InvalidFlowOutputApp implements Application {

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("InvalidFlowOutputApp")
      .setDescription("Invalid Flow output app")
      .noStream()
      .noDataSet()
      .withFlows()
        .add(new InvalidFlow())
      .noProcedure()
      .noMapReduce()
      .noWorkflow()
      .build();
  }

  /**
   *
   */
  public static final class InvalidFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("InvalidFlow")
        .setDescription("Invalid flow")
        .withFlowlets()
          .add("gen", new InvalidGenerator())
          .add("cons", new Consumer())
        .connect()
          .from("gen").to("cons")
        .build();
    }
  }

  /**
   *
   */
  public static final class InvalidGenerator extends AbstractFlowlet {

    private OutputEmitter<String> strOut;
    private OutputEmitter<Long> longOut;


    @Tick(delay = 1L, unit = TimeUnit.SECONDS)
    public void generate() {
      long ts = System.currentTimeMillis();
      strOut.emit(Long.toString(ts));
      longOut.emit(ts);
    }
  }

  /**
   *
   */
  public static final class Consumer extends AbstractFlowlet {

    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    public void process(String str) {
      LOG.info(str);
    }

    public void process(long ts) {
      LOG.info(Long.toString(ts));
    }
  }
}
