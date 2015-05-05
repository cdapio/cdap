/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * An app to test whether queue pending events metrics are emitted correctly.
 */
public class PendingMetricTestApp extends AbstractApplication {

  @Override
  public void configure() {
    addFlow(new TestPendingFlow());
  }

  public static class TestPendingFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("TestPendingFlow")
        .setDescription("A flow to test whether queue pending events metrics are emitted correctly.")
        .withFlowlets()
        .add("source", new Source())
        .add("forward-one", new ForwardOne())
        .add("forward-two", new ForwardTwo())
        .add("sink", new Sink())
        .connect()
        .from("source").to("forward-one")
        .from("source").to("forward-two")
        .from("forward-one").to("sink")
        .from("forward-two").to("sink")
        .build();
    }
  }

  public static class Source extends AbstractFlowlet {

    private static final Logger LOG = LoggerFactory.getLogger(Source.class);

    boolean generated = false;

    @Output("ints")
    private OutputEmitter<Integer> intOut;

    @Output("strings")
    private OutputEmitter<String> stringOut;

    @Tick(delay = 1L, unit = TimeUnit.MILLISECONDS)
    void generateOnce() throws InterruptedException {
      if (generated) {
        TimeUnit.MILLISECONDS.sleep(50);
        return;
      }
      String numEventsStr = getContext().getRuntimeArguments().get("count");
      int numEventsToGenerate = numEventsStr == null ? 2 : Integer.parseInt(numEventsStr);
      for (int i = 0; i < numEventsToGenerate; i++) {
        intOut.emit(i);
        stringOut.emit(Integer.toString(i));
      }
      generated = true;
      LOG.info("Emitted " + numEventsToGenerate + " events.");
    }
  }

  public static class ForwardOne extends AbstractFlowlet {

    private static final Logger LOG = LoggerFactory.getLogger(ForwardOne.class);

    private OutputEmitter<String> out;
    private File fileToWaitFor;

    @Override
    public void initialize(FlowletContext context) throws Exception {
      fileToWaitFor = getTempFile(context, "one");
    }

    @ProcessInput
    void processInt(int i) throws InterruptedException {
      waitForFile(fileToWaitFor, TimeUnit.SECONDS.toMillis(5));
      out.emit(Integer.toString(i));
      LOG.info("Forwarded int " + i);
    }
  }

  public static class ForwardTwo extends AbstractFlowlet {

    private static final Logger LOG = LoggerFactory.getLogger(ForwardTwo.class);

    private OutputEmitter<String> out;
    private File fileToWaitForInt;
    private File fileToWaitForString;

    @Override
    public void initialize(FlowletContext context) throws Exception {
      fileToWaitForInt = getTempFile(context, "two-i");
      fileToWaitForString = getTempFile(context, "two-s");
    }

    @ProcessInput
    void processInt(int i) throws InterruptedException {
      waitForFile(fileToWaitForInt, TimeUnit.SECONDS.toMillis(5));
      out.emit(Integer.toString(i));
      LOG.info("Forwarded int " + i);
    }

    @ProcessInput
    void processString(String s) throws InterruptedException {
      waitForFile(fileToWaitForString, TimeUnit.SECONDS.toMillis(5));
      out.emit(s);
      LOG.info("Forwarded string \"" + s + "\"");
    }
  }

  public static class Sink extends AbstractFlowlet {

    private static final Logger LOG = LoggerFactory.getLogger(ForwardTwo.class);

    private File fileToWaitFor;

    @Override
    public void initialize(FlowletContext context) throws Exception {
      fileToWaitFor = getTempFile(context, "three");
    }

    @ProcessInput
    void processString(String s) throws InterruptedException {
      waitForFile(fileToWaitFor, TimeUnit.SECONDS.toMillis(5));
      LOG.info("Received string \"" + s + "\"");
    }
  }

  private static void waitForFile(File file, long timeoutInMillis) throws InterruptedException {
    long timeoutTime = System.currentTimeMillis() + timeoutInMillis;
    while (timeoutTime > System.currentTimeMillis()) {
      if (file.exists()) {
        return;
      }
      TimeUnit.MILLISECONDS.sleep(50);
    }
    throw new RuntimeException("timeout waiting for file");
  }

  private static File getTempFile(FlowletContext context, String name) {
    String path = context.getRuntimeArguments().get("temp");
    Assert.assertNotNull(path);
    return new File(path, name);
  }
}
