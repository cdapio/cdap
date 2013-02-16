package com.continuuity.internal.app.runtime;

import com.continuuity.api.flow.flowlet.Flowlet;
import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.api.io.Schema;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.internal.io.ByteBufferInputStream;
import com.continuuity.internal.io.ReflectionDatumReader;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;

import javax.annotation.concurrent.NotThreadSafe;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 *
 */
@NotThreadSafe
public final class ReflectionProcessMethodInvoker<T> implements ProcessMethodInvoker {

  private final Flowlet flowlet;
  private final Method method;
  private final TypeToken<T> dataType;
  private final boolean needContext;
  private final ReflectionDatumReader<T> datumReader;
  private final ByteBufferInputStream input;
  private final BinaryDecoder decoder;


  public ReflectionProcessMethodInvoker(Flowlet flowlet, Method method, Schema schema, TypeToken<T> dataType) {
    this.flowlet = flowlet;
    this.method = method;
    this.dataType = dataType;
    this.needContext = method.getGenericParameterTypes().length == 2;
    this.datumReader = new ReflectionDatumReader<T>(schema, dataType);
    this.input = new ByteBufferInputStream(null);
    this.decoder = new BinaryDecoder(input);
  }

  @Override
  public void invoke(Schema sourceSchema, ByteBuffer data, InputContext context) {
    try {
      input.reset(data);
      T event = datumReader.read(decoder, sourceSchema);

      if (needContext) {
        method.invoke(flowlet, event, context);
      } else {
        method.invoke(flowlet, event);
      }

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
