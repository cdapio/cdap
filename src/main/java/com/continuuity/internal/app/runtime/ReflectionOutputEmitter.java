package com.continuuity.internal.app.runtime;

import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.io.Schema;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.io.Encoder;

import java.util.Map;

/**
 *
 */
final class ReflectionOutputEmitter implements OutputEmitter<Object> {

  private final Encoder encoder;

  ReflectionOutputEmitter(Schema schema) {
    this.encoder = new BinaryEncoder()
  }


  @Override
  public void emit(Object data) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void emit(Object data, Map<String, Object> partitions) {
    //To change body of implemented methods use File | Settings | File Templates.
  }
}
