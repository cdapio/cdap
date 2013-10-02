/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.flow;

import com.continuuity.api.annotation.Output;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.internal.lang.FieldVisitor;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;

/**
 *
 */
final class OutputEmitterFieldSetter extends FieldVisitor {

  private final OutputEmitterFactory outputEmitterFactory;

  OutputEmitterFieldSetter(OutputEmitterFactory outputEmitterFactory) {
    this.outputEmitterFactory = outputEmitterFactory;
  }

  @Override
  public void visit(Object instance, TypeToken<?> inspectType, TypeToken<?> declareType, Field field) throws Exception {
    if (OutputEmitter.class.equals(field.getType())) {
      TypeToken<?> emitterType = inspectType.resolveType(field.getGenericType());
      Preconditions.checkArgument(emitterType.getType() instanceof ParameterizedType,
                                  "Only ParameterizeType is supported for OutputEmitter.");

      TypeToken<?> outputType = inspectType.resolveType(((ParameterizedType) emitterType.getType())
                                                          .getActualTypeArguments()[0]);

      String outputName = field.isAnnotationPresent(Output.class) ?
        field.getAnnotation(Output.class).value() : FlowletDefinition.DEFAULT_OUTPUT;

      field.set(instance, outputEmitterFactory.create(outputName, outputType));
    }
  }
}
