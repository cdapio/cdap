package com.continuuity.internal.app.runtime;

import com.continuuity.api.flow.flowlet.Flowlet;
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
  private final SchemaCache schemaCache;
  private final boolean needContext;
  private final ReflectionDatumReader<T> datumReader;
  private final ByteBufferInputStream byteBufferInput;
  private final BinaryDecoder decoder;


  public ReflectionProcessMethodInvoker(Flowlet flowlet, Method method,
                                        TypeToken<T> dataType,
                                        Schema schema, SchemaCache schemaCache) {
    this.flowlet = flowlet;
    this.method = method;
    this.schemaCache = schemaCache;
    this.needContext = method.getGenericParameterTypes().length == 2;
    this.datumReader = new ReflectionDatumReader<T>(schema, dataType);
    this.byteBufferInput = new ByteBufferInputStream(null);
    this.decoder = new BinaryDecoder(byteBufferInput);

    if (!this.method.isAccessible()) {
      this.method.setAccessible(true);
    }
  }

  @Override
  public void invoke(InputDatum input) {
    try {
      ByteBuffer data = input.getData();
      Schema sourceSchema = schemaCache.get(data);

      byteBufferInput.reset(data);
      T event = datumReader.read(decoder, sourceSchema);

      if (needContext) {
        method.invoke(flowlet, event, input.getInputContext());
      } else {
        method.invoke(flowlet, event);
      }

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public String toString() {
    return method.toString();
  }
}
