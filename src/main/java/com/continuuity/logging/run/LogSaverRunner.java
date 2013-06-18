package com.continuuity.logging.run;

import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.logging.save.LogSaver;
import com.continuuity.weave.api.AbstractWeaveRunnable;
import com.continuuity.weave.api.WeaveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Weave wrapper for running LogSaver.
 */
public class LogSaverRunner extends AbstractWeaveRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(LogSaverRunner.class);
  private int instanceId;

  private String account;
  private OperationExecutor opex;
  private LogSaver logSaver;

  @Override
  public void initialize(WeaveContext context) {
    super.initialize(context);
    instanceId = context.getInstanceId();
    account = getArgument("account");
    try {
      logSaver = new LogSaver(opex, account, instanceId);
    } catch (IOException e) {
      LOG.error("Exception while initializing LogSaver", e);
    }
  }

  @Override
  public void run() {
    logSaver.startAndWait();
  }

  @Override
  public void stop() {
    logSaver.stopAndWait();
  }
}
