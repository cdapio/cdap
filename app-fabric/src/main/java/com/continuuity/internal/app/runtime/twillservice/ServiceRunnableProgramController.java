package com.continuuity.internal.app.runtime.twillservice;

import com.continuuity.internal.app.runtime.AbstractProgramController;
import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import org.apache.twill.api.TwillRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ServiceRunnableProgramController extends AbstractProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceRunnableProgramController.class);
  private final TwillRunnable twillRunnable;
  private final ServiceRunnableContext context;

  ServiceRunnableProgramController(String programName, String runnableName, ServiceRunnableContext context,
                                   TwillRunnable twillRunnable) {
    super(programName + ":" + runnableName, context.getRunId());
    this.twillRunnable = twillRunnable;
    this.context = context;
  }

  @Override
  protected void doSuspend() throws Exception {
    LOG.info("Suspending of ServiceRunnable not implemented : " + context);
  }

  @Override
  protected void doResume() throws Exception {
    LOG.info("Resuming of ServiceRunnable not implemented : " + context);
  }

  @Override
  protected void doStop() throws Exception {
    LOG.info("Stopping ServiceRunnable : " + context);
    twillRunnable.stop();
    twillRunnable.destroy();
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {
    if (!ProgramOptionConstants.INSTANCES.equals(name) || !(value instanceof Integer)) {
      return;
    }
    int instances = (Integer) value;
    LOG.info("Change ServiceRunnable instance count: " + context + ", new count is " + instances);
    changeInstanceCount(context, instances);
    LOG.info("ServiceRunnable instance count change: " + context + ", new count is " + instances);
  }

  private void changeInstanceCount(ServiceRunnableContext context, int instanceCount) {
    context.setInstanceCount(instanceCount);
  }
}
