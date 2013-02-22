/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.runtime.flow;

import com.continuuity.app.queue.InputDatum;
import com.continuuity.internal.app.runtime.PostProcess;

/**
 *
 */
interface ProcessMethod {

  boolean needsInput();

  PostProcess invoke(InputDatum input);
}
