/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.workflow;

/**
 *
 * @param <T>
 */
public interface FirstAction<T> {

  T startWith(WorkFlowAction action);
}
