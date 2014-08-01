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
package com.continuuity.test.app;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.Tick;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.dataset.table.Get;
import com.continuuity.api.data.dataset.table.Put;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.FlowletException;
import com.continuuity.api.flow.flowlet.OutputEmitter;

import java.util.concurrent.TimeUnit;

/**
 * An app that access DataSet in initialize method.
 */
public class DataSetInitApp implements Application {

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("DataSetInitApp")
      .setDescription("DataSetInitApp")
      .noStream()
      .withDataSets()
        .add(new Table("conf"))
      .withFlows()
        .add(new DataSetFlow())
      .noProcedure()
      .noMapReduce()
      .noWorkflow()
      .build();
  }

  /**
   * Flow with a data set.
   */
  public static final class DataSetFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("DataSetFlow")
        .setDescription("DataSetFlow")
        .withFlowlets()
          .add(new Generator())
          .add(new Consumer())
        .connect()
          .from(new Generator()).to(new Consumer())
        .build();
    }
  }

  /**
   * Flowlet that emits data every now and then.
   */
  public static final class Generator extends AbstractFlowlet {

    @UseDataSet("conf")
    private Table confTable;
    private OutputEmitter<String> output;


    @Override
    public void initialize(FlowletContext context) throws FlowletException {
      confTable.put(new Put("key", "column", "generator"));
    }

    @Tick(delay = 10, unit = TimeUnit.MINUTES)
    public void generate() {
      output.emit("test");
    }
  }

  /**
   * Flowlet that consumes input.
   */
  public static final class Consumer extends AbstractFlowlet {

    @UseDataSet("conf")
    private Table confTable;

    @ProcessInput (maxRetries = 0)
    public void process(String str) {
      if (!"generator".equals(confTable.get(new Get("key", "column")).getString("column"))) {
        throw new IllegalArgumentException("Illegal value");
      }
    }
  }
}
