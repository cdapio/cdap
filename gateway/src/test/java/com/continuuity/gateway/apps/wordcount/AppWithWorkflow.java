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

package com.continuuity.gateway.apps.wordcount;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.workflow.AbstractWorkflowAction;
import com.continuuity.api.workflow.Workflow;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An app with a workflow.
 */
public class AppWithWorkflow implements Application {

  @Override
  public ApplicationSpecification configure() {
    try {
      return ApplicationSpecification.Builder.with()
        .setName("AppWithWorkflow")
        .setDescription("Sample application")
        .noStream()
        .withDataSets()
        .add(new ObjectStore<String>("input", String.class))
        .add(new ObjectStore<String>("output", String.class))
        .noFlow()
        .noProcedure()
        .noMapReduce()
        .withWorkflows()
        .add(new SampleWorkflow())
        .build();
    } catch (UnsupportedTypeException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Sample workflow. has a dummy action.
   */
  public static class SampleWorkflow implements Workflow {

    @Override
    public WorkflowSpecification configure() {
      return WorkflowSpecification.Builder.with()
        .setName("SampleWorkflow")
        .setDescription("SampleWorkflow description")
        .startWith(new DummyAction())
        .last(new DummyAction())
        .build();
    }
  }

  /**
   * DummyAction
   */
  public static class DummyAction extends AbstractWorkflowAction {
    private static final Logger LOG = LoggerFactory.getLogger(DummyAction.class);
    @Override
    public void run() {
      LOG.info("Ran dummy action");
    }
  }
}
