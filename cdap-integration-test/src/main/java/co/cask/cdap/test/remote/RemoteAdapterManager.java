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

package co.cask.cdap.test.remote;

import co.cask.cdap.client.AdapterClient;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.AbstractAdapterManager;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class RemoteAdapterManager extends AbstractAdapterManager {

  protected final Id.Adapter adapterId;

  private final AdapterClient adapterClient;

  public RemoteAdapterManager(Id.Adapter adapterId, AdapterClient adapterClient) {
    this.adapterId = adapterId;
    this.adapterClient = adapterClient;
  }

  @Override
  public void start() throws IOException {
    // TODO: implement
  }

  @Override
  public void stop() throws IOException {
    // TODO: implement
  }

  @Override
  public List<RunRecord> getRuns() {
    // TODO: implement

    return null;
  }

  @Override
  public RunRecord getRun(String runId) {
    // TODO: implement
    return null;
  }

  @Override
  public void delete() throws Exception {
    // TODO: implement
  }
}
