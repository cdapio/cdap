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

package co.cask.cdap.test.internal;

import co.cask.cdap.internal.AppFabricClient;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.AbstractAdapterManager;
import co.cask.cdap.test.ApplicationManager;

import java.io.IOException;
import java.util.List;

/**
 * A local implementation of {@link ApplicationManager}.
 */
public class DefaultAdapterManager extends AbstractAdapterManager {

  private final AppFabricClient appFabricClient;
  private final Id.Adapter adapterId;

  public DefaultAdapterManager(AppFabricClient appFabricClient, Id.Adapter adapterId) {
    this.adapterId = adapterId;
    this.appFabricClient = appFabricClient;
  }

  @Override
  public void start() throws IOException {
    appFabricClient.startAdapter(adapterId);
  }

  @Override
  public void stop() throws IOException {
    appFabricClient.stopAdapter(adapterId);
  }

  @Override
  public List<RunRecord> getRuns() {
    return appFabricClient.getAdapterRuns(adapterId);
  }

  @Override
  public RunRecord getRun(String runId) {
    return appFabricClient.getAdapterRun(adapterId, runId);
  }

}
