/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.deploy;

import com.continuuity.internal.app.deploy.ProgramTerminator;

/**
 * Factory for creating deployment {@link Manager}.
 *
 * @param <I> Input type of the deployment.
 * @param <O> Output type of the deployment.
 */
public interface ManagerFactory<I, O> {
  Manager<I, O> create(ProgramTerminator handler);
}
