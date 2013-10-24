/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.workflow;

import com.continuuity.api.mapreduce.MapReduceContext;

import java.util.concurrent.Callable;

/**
 *
 */
public interface MapReduceRunnerFactory {

  Callable<MapReduceContext> create(String name);
}
