/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.deploy;

import com.continuuity.app.Id;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

/**
 * Interface to represent deployment manager.
 *
 * @param <I> Input type.
 * @param <O> Output type.
 */
public interface Manager<I, O> {
  ListenableFuture<O> deploy(Id.Account id, @Nullable String appId, I input) throws Exception;
}
