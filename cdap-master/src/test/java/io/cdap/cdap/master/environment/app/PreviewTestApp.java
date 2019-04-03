/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.app;

import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.customaction.AbstractCustomAction;
import io.cdap.cdap.api.workflow.AbstractWorkflow;

/**
 * An app with a workflow that just writes a single tracer value. Used to test preview.
 */
public class PreviewTestApp extends AbstractApplication {
  public static final String TRACER_NAME = "trace";
  public static final String TRACER_KEY = "k";
  public static final String TRACER_VAL = "v";

  @Override
  public void configure() {
    addWorkflow(new TestWorkflow());
  }

  /**
   * Workflow that has a single action that writes to a tracer.
   */
  public static class TestWorkflow extends AbstractWorkflow {
    public static final String NAME = TestWorkflow.class.getSimpleName();

    @Override
    protected void configure() {
      setName(NAME);
      addAction(new TracerWriter());
    }
  }

  /**
   * Writes a value to the tracer.
   */
  public static class TracerWriter extends AbstractCustomAction {

    @Override
    public void run() {
      getContext().getDataTracer(TRACER_NAME).info(TRACER_KEY, TRACER_VAL);
    }
  }
}
