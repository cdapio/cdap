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
import co.cask.cdap.common.id.Id;
import co.cask.cdap.internal.AppFabricClient;
import co.cask.cdap.test.AbstractProgramManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.MetricsManager;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * A default implementation of {@link co.cask.cdap.test.FlowManager}.
 */
public class DefaultFlowManager extends AbstractProgramManager<FlowManager> implements FlowManager {
  private final AppFabricClient appFabricClient;
  private final MetricsManager metricsManager;

  public DefaultFlowManager(Id.Program programId, AppFabricClient appFabricClient,
                            DefaultApplicationManager applicationManager, MetricsManager metricsManager) {
    super(programId, applicationManager);
    this.appFabricClient = appFabricClient;
    this.metricsManager = metricsManager;
  }

  @Override
  public void setFlowletInstances(String flowletName, int instances) {
    Preconditions.checkArgument(instances > 0, "Instance counter should be > 0.");
    try {
      appFabricClient.setFlowletInstances(programId.getNamespace(), programId.getApplication(), programId.getProgram(),
                                          flowletName, instances);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public int getFlowletInstances(String flowletName) {
    try {
      return appFabricClient.getFlowletInstances(programId.getNamespace(), programId.getApplication(),
                                                 programId.getProgram(), flowletName).getInstances();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public RuntimeMetrics getFlowletMetrics(String flowletId) {
    return metricsManager.getFlowletMetrics(programId.getNamespace(), programId.getApplication(),
                                            programId.getProgram(), flowletId);
  }
}
