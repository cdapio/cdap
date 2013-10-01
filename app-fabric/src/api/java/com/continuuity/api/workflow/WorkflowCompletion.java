/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.workflow;

/**
 * The WorkflowCompletion interface provides onSuccess() and onFailure() methods.
 */
public interface WorkflowCompletion {

  void onSuccess(WorkflowSpecification specification);

  void onFailure(WorkflowSpecification specification, Throwable cause);
}
