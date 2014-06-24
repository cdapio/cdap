package com.continuuity.internal.app.runtime.service;

import com.continuuity.internal.app.runtime.AbstractProgramController;
import org.apache.twill.api.Command;
import org.apache.twill.internal.BasicTwillContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Program Controller for Service runnable
 */
public class InMemoryRunnableProgramController extends AbstractProgramController {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryRunnableProgramController.class);
  private InMemoryRunnableDriver driver;

  public InMemoryRunnableProgramController(String serviceName, String runnableName, BasicTwillContext twillContext,
                                           InMemoryRunnableDriver driver) {
    super(serviceName + ":" + runnableName, twillContext.getRunId());
    this.driver = driver;
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

  @SuppressWarnings(value="unchecked")
  @Override
  protected void doCommand(final String name, final Object value) throws Exception {
    try {
      final Map<String, String> kvMap = (Map<String, String>) value;
      driver.handleCommand(new Command() {
        @Override
        public String getCommand() {
          return name;
        }

        @Override
        public Map<String, String> getOptions() {
          return kvMap;
        }
      });
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }
}
