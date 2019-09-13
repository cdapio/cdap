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

package io.cdap.cdap.internal.app.services;

import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.customaction.AbstractCustomAction;
import io.cdap.cdap.api.mapreduce.AbstractMapReduce;
import io.cdap.cdap.api.spark.AbstractSpark;
import io.cdap.cdap.api.workflow.AbstractWorkflow;

/**
 * A testing application for testing workflow program state transition.
 */
public class ProgramStateWorkflowApp extends AbstractApplication {

  @Override
  public void configure() {
    addWorkflow(new ProgramStateWorkflow());
    addMapReduce(new ProgramStateMR());
    addSpark(new ProgramStateSpark());
    addSpark(new ProgramStateSpark2());
  }

  /**
   *
   */
  public static final class ProgramStateWorkflow extends AbstractWorkflow {

    @Override
    protected void configure() {
      addAction(new ProgramStateAction());
      fork()
        .addMapReduce(ProgramStateMR.class.getSimpleName())
        .also()
        .addSpark(ProgramStateSpark.class.getSimpleName())
        .join();
      addSpark(ProgramStateSpark2.class.getSimpleName());
    }
  }

  /**
   *
   */
  public static final class ProgramStateMR extends AbstractMapReduce {
    // Empty. We are not running anything
  }

  /**
   *
   */
  public static final class ProgramStateSpark extends AbstractSpark {
    // Empty. We are not running anything
  }

  /**
   *
   */
  public static final class ProgramStateSpark2 extends AbstractSpark {
    // Empty. We are not running anything
  }

  /**
   *
   */
  public static final class ProgramStateAction extends AbstractCustomAction {

    @Override
    public void run() {
      // No-op
    }
  }
}
