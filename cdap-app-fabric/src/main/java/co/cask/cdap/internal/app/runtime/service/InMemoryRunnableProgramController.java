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

package co.cask.cdap.internal.app.runtime.service;

import co.cask.cdap.internal.app.runtime.AbstractProgramController;
import com.google.common.collect.ImmutableList;
import org.apache.twill.api.TwillContext;
import org.apache.twill.discovery.Discoverable;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Program Controller for Service runnable
 */
final class InMemoryRunnableProgramController extends AbstractProgramController {
  private InMemoryRunnableDriver driver;
  private final ConcurrentLinkedQueue<Discoverable> discoverables;

  InMemoryRunnableProgramController(String serviceName, String runnableName,
                                    TwillContext twillContext, InMemoryRunnableDriver driver,
                                    ConcurrentLinkedQueue<Discoverable> discoverables) {
    super(serviceName + ":" + runnableName, twillContext.getRunId());
    this.driver = driver;
    this.discoverables = discoverables;
  }

  @Override
  protected void doSuspend() throws Exception {
    //no-op
  }

  @Override
  protected void doResume() throws Exception {
    //no-op
  }

  @Override
  protected void doStop() throws Exception {
    driver.stopAndWait();
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {
    //no-op
  }

  public List<Discoverable> getDiscoverables() {
    return ImmutableList.copyOf(this.discoverables);
  }
}
