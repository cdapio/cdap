/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.ssh;


import co.cask.cdap.api.Config;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowNodeState;

import java.util.Map;

public class SSHApp extends AbstractApplication<SSHApp.SSHAppConfig> {

  public static class SSHAppConfig extends Config {

  }

  @Override
  public void configure() {
    setName("SSH Test Application");
    setDescription("A simple SSH application demonstrating the SSH workflow action.");
    createDataset("privateKeyFile", FileSet.class);
    addWorkflow(new SSHAppWorkflow());
  }

  private static class SSHAppWorkflow extends AbstractWorkflow {
    @Override
    public void initialize(WorkflowContext context) throws Exception {
      super.initialize(context);
    }

    @Override
    public void configure() {
      setName("SSHWorkflow");
      setDescription("Workflow that utilizes the SSHAction custom action.");
      String privateKeyFile = "/Users/Kashif/.ssh/id_rsa";
      addAction(new SSHAction(privateKeyFile));
    }

    @Override
    public void destroy() {
      boolean isWorkflowSuccessful = getContext().isSuccessful();
      Map<String, WorkflowNodeState> nodeStates = getContext().getNodeStates();
    }
  }

}
