/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.flow;

import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.google.common.reflect.TypeToken;

/**
 * A package only factory for creating instance of {@link OutputEmitter}.
 */
interface OutputEmitterFactory {
  <T> OutputEmitter<T> create(String outputName, TypeToken<T> type);
}
