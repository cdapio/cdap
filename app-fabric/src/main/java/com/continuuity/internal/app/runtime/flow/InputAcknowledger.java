/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.flow;

import com.continuuity.api.data.OperationException;

/**
 * Interface to represent sending an ack on a input.
 */
interface InputAcknowledger {
  void ack() throws OperationException;
}
