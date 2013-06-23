/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.deploy;

import com.continuuity.app.Id;
import com.google.common.util.concurrent.ListenableFuture;

/**
 *
 */
public interface Manager<I, O> {
  ListenableFuture<O> deploy(Id.Account id, I input) throws Exception;
}
