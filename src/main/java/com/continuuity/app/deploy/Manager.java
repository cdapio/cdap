/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.deploy;

import com.continuuity.app.program.Program;
import com.continuuity.filesystem.Location;
import com.continuuity.pipeline.Pipeline;

/**
 *
 */
public interface Manager {
  Pipeline deploy(Location deployedJar) throws Exception;
}
