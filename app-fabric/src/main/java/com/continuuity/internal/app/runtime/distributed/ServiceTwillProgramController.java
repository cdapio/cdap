package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import org.apache.twill.api.TwillController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *  A ProgramController for Services that are launched through Twill.
 */
final class ServiceTwillProgramController extends AbstractTwillProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceTwillProgramController.class);

  private final Lock lock;
  private final DistributedServiceRunnableInstanceUpdater instanceUpdater;

  ServiceTwillProgramController(String programId, TwillController controller,
                                DistributedServiceRunnableInstanceUpdater instanceUpdater) {
    super(programId, controller);
    this.lock = new ReentrantLock();
    this.instanceUpdater = instanceUpdater;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void doCommand(String name, Object value) throws Exception {
    if (!ProgramOptionConstants.RUNNABLE_INSTANCES.equals(name) || !(value instanceof Map)) {
      return;
    }

    Map<String, String> command = (Map<String, String>) value;
    lock.lock();
    try {
      changeInstances(command.get("runnable"),
                      Integer.valueOf(command.get("newInstances")),
                      Integer.valueOf(command.get("oldInstances")));
    } catch (Throwable t) {
      LOG.error(String.format("Failed to change instances: %s", command), t);
    } finally {
      lock.unlock();
    }
  }

  private synchronized void changeInstances(String runnableId, int newInstanceCount, int oldInstanceCount)
    throws Exception {
    instanceUpdater.update(runnableId, newInstanceCount, oldInstanceCount);
  }
}
