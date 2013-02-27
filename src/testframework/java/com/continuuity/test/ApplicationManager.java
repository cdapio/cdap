package com.continuuity.test;

/**
 *
 */
public interface ApplicationManager {

  FlowManager startFlow(String flowName);

  ProcedureManager startProcedure(String procedureName);

  StreamWriter getStreamWriter(String streamName);
}
