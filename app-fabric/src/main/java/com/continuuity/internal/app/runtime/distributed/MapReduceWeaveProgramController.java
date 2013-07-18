/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.weave.api.WeaveController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Weave program controller for MapReduce program
 */
final class MapReduceWeaveProgramController extends AbstractWeaveProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceWeaveProgramController.class);

  MapReduceWeaveProgramController(String programName, WeaveController controller) {
    super(programName, controller);
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {
    // MapReduce doesn't have any command for now.
    LOG.info("Command ignored for mapreduce controller: {}, {}", name, value);
  }
}
