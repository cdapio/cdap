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

package co.cask.cdap;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.workflow.AbstractWorkflow;

/**
 *  App to test adapter lifecycle.
 */
public class AdapterApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("AdapterApp");
    addStream(new Stream("mySource"));
    setDescription("Application for to test Adapter lifecycle");
    addWorkflow(new AdapterWorkflow());
    addMapReduce(new DummyMapReduceJob());
  }

  public static class AdapterWorkflow extends AbstractWorkflow {
    @Override
    protected void configure() {
      setName("AdapterWorkflow");
      setDescription("Workflow to test Adapter");
      addMapReduce(DummyMapReduceJob.NAME);
    }
  }

  public static class DummyMapReduceJob extends AbstractMapReduce {
    public static final String NAME = "DummyMapReduceJob";
    @Override
    protected void configure() {
      setName(NAME);
      setDescription("Mapreduce that does nothing (and actually doesn't run) - it is here to test Adapter lifecycle");
    }
  }
}
