/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.test.app;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.FlowletException;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * An app that access DataSet in initialize method.
 */
public class DataSetInitApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("DataSetInitApp");
    setDescription("DataSetInitApp");
    createDataset("conf", Table.class);
    addFlow(new DataSetFlow());
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
    public void initialize(FlowletContext context) throws Exception {
      super.initialize(context);
      // Put some data in initialize, expect be able to read it back in the next flowlet.
      confTable.put(new Put("key", "column", "generator"));
    }

    @Tick(delay = 10, unit = TimeUnit.MINUTES)
    public void generate() {
      output.emit("test" + getContext().getInstanceId());
    }
  }

  /**
   * Flowlet that consumes input.
   */
  public static final class Consumer extends AbstractFlowlet {

    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    @UseDataSet("conf")
    private Table confTable;

    @ProcessInput (maxRetries = 0)
    public void process(String str) {
      // Read data from Dataset, supposed to be written from Generator initialize method
      if (!"generator".equals(confTable.get(new Get("key", "column")).getString("column"))) {
        throw new IllegalArgumentException("Illegal value");
      }

      LOG.info("Received: {}", str);
    }
  }
}
