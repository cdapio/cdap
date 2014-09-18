/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.internal.app.runtime.flow;

import co.cask.cdap.api.annotation.Output;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.internal.lang.FieldVisitor;
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
