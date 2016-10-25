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
package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.proto.id.ProgramId;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A ProgramController for flow program that are launched through Twill.
 */
final class FlowTwillProgramController extends AbstractTwillProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(FlowTwillProgramController.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();

  private final Lock lock;
  private final DistributedFlowletInstanceUpdater instanceUpdater;

  FlowTwillProgramController(ProgramId programId, TwillController controller,
                             DistributedFlowletInstanceUpdater instanceUpdater, RunId runId) {
    super(programId, controller, runId);
    this.lock = new ReentrantLock();
    this.instanceUpdater = instanceUpdater;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void doCommand(String name, Object value) throws Exception {
    if (!ProgramOptionConstants.INSTANCES.equals(name) || !(value instanceof Map)) {
      return;
    }
    Map<String, String> command = (Map<String, String>) value;
    lock.lock();
    try {
      changeInstances(command.get("flowlet"),
                      Integer.valueOf(command.get("newInstances")),
                      GSON.fromJson(command.get("oldFlowSpec"), FlowSpecification.class));
    } catch (Throwable t) {
      LOG.error("Fail to change instances. Terminating flow: {}", command, t);
      stop();
      throw t;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Change the number of instances of the running flowlet. Notice that this method needs to be
   * synchronized as change of instances involves multiple steps that need to be completed all at once.
   * @param flowletId Name of the flowlet.
   * @param newInstanceCount New instance count.
   * @param flowSpec The flow specification before the instance change
   * @throws java.util.concurrent.ExecutionException
   * @throws InterruptedException
   */
  private synchronized void changeInstances(String flowletId, int newInstanceCount,
                                            FlowSpecification flowSpec) throws Exception {
    instanceUpdater.update(flowletId, newInstanceCount, flowSpec);
  }
}
