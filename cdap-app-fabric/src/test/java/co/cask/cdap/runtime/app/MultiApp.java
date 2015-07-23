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

package co.cask.cdap.runtime.app;

import co.cask.cdap.api.annotation.Output;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class MultiApp extends AbstractApplication {

  public static final byte[] KEY = new byte[] {'k', 'e', 'y'};

  @Override
  public void configure() {
    setName("MultiApp");
    setDescription("MultiApp");
    createDataset("accumulated", KeyValueTable.class);
    addFlow(new MultiFlow());
  }

  /**
   *
   */
  public static final class MultiFlow extends AbstractFlow {

    @Override
    protected void configureFlow() {
      setName("MultiFlow");
      setDescription("MultiFlow");
      addFlowlet("gen", new Generator());
      addFlowlet("c1", new Consumer(), 2);
      addFlowlet("c2", new Consumer(), 2);
      addFlowlet("c3", new ConsumerStr(), 2);
      connect("gen", "c1");
      connect("gen", "c2");
      connect("gen", "c3");
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
