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

package co.cask.cdap.test.internal;

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.RuntimeStats;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * A default implementation of {@link co.cask.cdap.test.FlowManager}.
 */
public class DefaultFlowManager implements FlowManager {

  private final AppFabricClient appFabricClient;
  private final DefaultApplicationManager.ProgramId flowID;
  private final DefaultApplicationManager applicationManager;
  private final String flowName;
  private final String applicationId;
  private final String namespace;

  public DefaultFlowManager(String namespace, String flowName, DefaultApplicationManager.ProgramId flowID,
                            String applicationId, AppFabricClient appFabricClient,
                            DefaultApplicationManager applicationManager) {
    this.namespace = namespace;
    this.applicationManager = applicationManager;
    this.applicationId = applicationId;
    this.appFabricClient = appFabricClient;
    this.flowID = flowID;
    this.flowName = flowName;
  }

  @Override
  public void setFlowletInstances(String flowletName, int instances) {
    Preconditions.checkArgument(instances > 0, "Instance counter should be > 0.");
    try {
      appFabricClient.setFlowletInstances(namespace, applicationId, flowName, flowletName, instances);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public RuntimeMetrics getFlowletMetrics(String flowletId) {
    return RuntimeStats.getFlowletMetrics(namespace, applicationId, flowName, flowletId);
  }

  @Override
  public void stop() {
    applicationManager.stopProgram(flowID);
  }

  @Override
  public boolean isRunning() {
    return applicationManager.isRunning(flowID);
  }
}
