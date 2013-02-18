/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.runtime;

/**
 *
 */
public interface ProcessMethod {

  PostProcess invoke(InputDatum input);
}
