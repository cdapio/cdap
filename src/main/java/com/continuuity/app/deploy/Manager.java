/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.deploy;

import com.continuuity.filesystem.Location;
import com.google.common.util.concurrent.ListenableFuture;

/**
 *
 */
public interface Manager {
  ListenableFuture<?> deploy(Location deployedJar) throws Exception;
}
