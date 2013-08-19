/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.runtime.flow;

import com.continuuity.app.queue.InputDatum;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.internal.app.runtime.PostProcess;
import com.google.common.base.Function;

import java.nio.ByteBuffer;

/**
 *
 */
interface ProcessMethod<T> {

  boolean needsInput();

  PostProcess invoke(TransactionAgent txAgent, InputDatum input, Function<ByteBuffer, T> inputDatumTransformer);
}
