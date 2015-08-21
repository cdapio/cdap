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

import co.cask.cdap.api.annotation.Batch;
import co.cask.cdap.api.annotation.Output;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.InputContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class GenSinkApp2 extends AbstractApplication {

  @Override
  public void configure() {
    setName("GenSinkApp");
    setDescription("GenSinkApp desc");
    addFlow(new GenSinkFlow());
  }

  /**
   *
   */
  public static final class GenSinkFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("GenSinkFlow")
        .setDescription("GenSinkFlow desc")
        .withFlowlets()
        .add(new GenFlowlet())
        .add(new SinkFlowlet())
        .add(new BatchSinkFlowlet())
        .connect()
        .from(new GenFlowlet()).to(new SinkFlowlet())
        .from(new GenFlowlet()).to(new BatchSinkFlowlet())
        .build();
    }
  }

  /**
   * @param <T>
   * @param <U>
   */
  public abstract static class GenFlowletBase<T, U> extends AbstractFlowlet {

    protected OutputEmitter<T> output;

    @Output("batch")
    protected OutputEmitter<U> batchOutput;

    @Tick(delay = 1L, unit = TimeUnit.DAYS)
    public void generate() throws Exception {
      // No-op
    }
  }

  /**
   *
   */
  public static final class GenFlowlet extends GenFlowletBase<String, Integer> {

    private int i;

    @Tick(delay = 1L, unit = TimeUnit.NANOSECONDS)
    public void generate() throws Exception {
      if (i < 100) {
        output.emit("Testing " + ++i);
        batchOutput.emit(i);
        if (i == 10) {
          throw new IllegalStateException("10 hitted");
        }
      }
    }
  }

  /**
   * @param <T>
   * @param <U>
   */
  public abstract static class SinkFlowletBase<T, U> extends AbstractFlowlet {
    private static final Logger LOG = LoggerFactory.getLogger(SinkFlowletBase.class);

    @ProcessInput
    public void process(T event, InputContext context) throws InterruptedException {
      LOG.info(event.toString());
    }

    @ProcessInput
    public void process(T event) throws InterruptedException {
      // This method would violate the flowlet construct as same input name for same input type.
      // Children classes would override this without the @ProcessInput
    }

    @Batch(10)
    @ProcessInput("batch")
    public void processBatch(U event) {
      LOG.info(event.toString());
    }
  }

  /**
   *
   */
  public static final class SinkFlowlet extends SinkFlowletBase<String, Integer> {
    @ProcessInput
    public void process(String event, InputContext context) throws InterruptedException {
      super.process(event, context);
    }

    @Override
    public void process(String event) throws InterruptedException {
      // Nothing. Just override to avoid deployment failure.
    }
  }

  /**
   * Consume batch event of type integer. This is for batch consume with Iterator.
   */
  public static final class BatchSinkFlowlet extends AbstractFlowlet {
    private static final Logger LOG = LoggerFactory.getLogger(BatchSinkFlowlet.class);

    @Batch(10)
    @ProcessInput("batch")
    public void processBatch(Iterator<Integer> events) {
      while (events.hasNext()) {
        LOG.info("Iterator batch: {}", events.next().toString());
      }
    }
  }
}

