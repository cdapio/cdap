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
import co.cask.cdap.api.annotation.Retry;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;

/**
 * An app with a flowlet that overwrites the onChangeInstances method.
 */
public class ChangeFlowletInstancesApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("DataSetInitApp");
    setDescription("DataSetInitApp");
    createDataset("conf", Table.class);
    addFlow(new DataSetFlow());
    addStream(new Stream("generatorStream"));
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
        .fromStream("generatorStream").to(new Generator())
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

    @ProcessInput
    public void process(StreamEvent event) {
      // No-op
    }

    @Override
    public void onChangeInstances(FlowletContext flowletContext, int previousInstancesCount) throws Exception {
      if (flowletContext.getInstanceId() == 0) {
        confTable.put(new Put("key", "column", "generator"));
      }
      output.emit("test");
    }
  }

  /**
   * Flowlet that consumes input.
   */
  public static final class Consumer extends AbstractFlowlet {
    private static int tries = 0;

    @UseDataSet("conf")
    private Table confTable;

    @ProcessInput (maxRetries = 0)
    public void process(String str) {
      if (!"generator".equals(confTable.get(new Get("key", "column")).getString("column"))) {
        throw new IllegalArgumentException("Illegal value");
      }
      if (!str.equals("test")) {
        throw new IllegalArgumentException("Illegal value");
      }
    }

    @Override
    @Retry(maxRetries = 5)
    public void onChangeInstances(FlowletContext flowletContext, int previousInstancesCount) throws Exception {
      while (tries++ < 5) {
        throw new Exception("Test exception");
      }
      confTable.put(new Put("key2", "column", "consumer"));
    }
  }
}

