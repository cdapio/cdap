/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import org.apache.twill.api.TwillController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Twill program controller for MapReduce program
 */
final class MapReduceTwillProgramController extends AbstractTwillProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceTwillProgramController.class);

  MapReduceTwillProgramController(String programName, TwillController controller) {
    super(programName, controller);
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {
    // MapReduce doesn't have any command for now.
    LOG.info("Command ignored for mapreduce controller: {}, {}", name, value);
  }
}
