/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
package co.cask.cdap.examples.purchase;

import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a simple Workflow with one Workflow action to run the PurchaseHistoryBuilder 
 * MapReduce with a schedule that runs every day at 4:00 A.M.
 */
public class PurchaseHistoryWorkflow extends AbstractWorkflow {

  private final String name;

  public PurchaseHistoryWorkflow(String name) {
    this.name = name;
  }

  @Override
  public void configure() {
      setName(name);
      setDescription("PurchaseHistoryWorkflow description");
//      addMapReduce("PurchaseHistoryBuilder");
    addAction(new LogAction(name));
  }

  private static final class LogAction extends AbstractWorkflowAction {
    private static final Logger LOG = LoggerFactory.getLogger(PurchaseHistoryWorkflow.class);

    private final String name;

    public LogAction(String name) {
      this.name = name;
    }

    @Override
    public void run() {
      for (int i = 0; i < 500; i++) {
        LOG.info("Workflow action {} log {}", name, i);
      }
    }
  }
}
