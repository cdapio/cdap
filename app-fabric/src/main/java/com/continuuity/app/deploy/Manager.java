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

  /**
   * Executes a pipeline for deploying an input.
   *
   * @param id account id to which the archive is deployed.
   * @param appId application id to be used to override app name provided by app spec. If null, name of app spec is used
   * @param input the input to the deployment pipeline
   * @return A future of Application with Programs.
   */
  ListenableFuture<O> deploy(Id.Account id, @Nullable String appId, I input) throws Exception;
}
