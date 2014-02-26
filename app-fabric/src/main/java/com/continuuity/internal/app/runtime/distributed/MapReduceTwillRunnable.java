/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.internal.app.runtime.batch.MapReduceProgramRunner;
import org.apache.hadoop.mapred.YarnClientProtocolProvider;

/**
 * Wraps {@link MapReduceProgramRunner} to be run via Twill
 */
final class MapReduceTwillRunnable extends AbstractProgramTwillRunnable<MapReduceProgramRunner> {
  // NOTE: DO NOT REMOVE.  Though it is unused, the dependency is needed when submitting the mapred job.
  private YarnClientProtocolProvider provider;

  MapReduceTwillRunnable(String name, String hConfName, String cConfName) {
    super(name, hConfName, cConfName);
  }

  @Override
  protected Class<MapReduceProgramRunner> getProgramClass() {
    return MapReduceProgramRunner.class;
  }
}
