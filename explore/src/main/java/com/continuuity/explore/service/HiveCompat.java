package com.continuuity.explore.service;

import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * Handles compatibility issues between various versions of Hive.
 */
public class HiveCompat {
  private static final Logger LOG = LoggerFactory.getLogger(HiveCompat.class);
  private static final boolean hasOperationStatusClass;

  static {
    boolean hasStatusClass = false;
    try {
      Method operationStatusMethod = CLIService.class.getMethod("getOperationStatus", OperationHandle.class);

      hasStatusClass =
        operationStatusMethod.getReturnType().getCanonicalName().equals("org.apache.hive.service.cli.OperationStatus");
    } catch (NoSuchMethodException e) {
      LOG.error("Cannot find Hive CLIService.getOperationStatus method to determine Hive compatibility", e);
    } finally {
      hasOperationStatusClass = hasStatusClass;
    }
  }

  public static Status getStatus(CLIService cliService, OperationHandle operationHandle) throws HiveSQLException {
    Object status = cliService.getOperationStatus(operationHandle);

    // Hive 12 returns OperationState, and Hive 13 returns OperationStatus
    if (!hasOperationStatusClass) {
      OperationState operationState = (OperationState) status;
      return new Status(Status.State.valueOf(operationState.toString()), operationHandle.hasResultSet());
    } else {
      OperationStatus operationStatus = (OperationStatus) status;
      return new Status(Status.State.valueOf(operationStatus.getState().toString()), operationHandle.hasResultSet());
    }
  }
}
