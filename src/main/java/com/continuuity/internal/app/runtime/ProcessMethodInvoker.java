package com.continuuity.internal.app.runtime;

import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.api.io.Schema;

import java.nio.ByteBuffer;

/**
 *
 */
public interface ProcessMethodInvoker {

  void invoke(Schema sourceSchema, ByteBuffer data, InputContext context);
}
