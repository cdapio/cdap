/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.internal.app.runtime.batch.MapReduceProgramRunner;

/**
 * Wraps {@link MapReduceProgramRunner} to be run via Weave
 */
final class MapReduceWeaveRunnable extends AbstractProgramWeaveRunnable<MapReduceProgramRunner> {

  MapReduceWeaveRunnable(String name, String hConfName, String cConfName) {
    super(name, hConfName, cConfName);
  }

  @Override
  protected Class<MapReduceProgramRunner> getProgramClass() {
    return MapReduceProgramRunner.class;
  }
}
