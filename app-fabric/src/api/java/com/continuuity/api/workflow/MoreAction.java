/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.workflow;

/**
 *
 * @param <T>
 */
public interface MoreAction<T> {

  MoreAction<T> then(WorkFlowAction action);

  T last(WorkFlowAction action);
}
