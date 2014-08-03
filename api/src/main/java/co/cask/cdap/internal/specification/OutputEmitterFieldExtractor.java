/*
 * Copyright 2014 Cask, Inc.
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
package co.cask.cdap.internal.specification;

import co.cask.cdap.api.annotation.Output;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.internal.lang.FieldVisitor;
import co.cask.cdap.internal.lang.Reflections;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public final class OutputEmitterFieldExtractor extends FieldVisitor {

  private final Map<String, Set<Type>> outputTypes;

  public OutputEmitterFieldExtractor(Map<String, Set<Type>> outputTypes) {
    this.outputTypes = outputTypes;
  }

  @Override
  public void visit(Object instance, TypeToken<?> inspectType, TypeToken<?> declareType, Field field) throws Exception {
    if (!OutputEmitter.class.equals(field.getType())) {
      return;
    }
    Type emitterType = inspectType.resolveType(field.getGenericType()).getType();
    Preconditions.checkArgument(emitterType instanceof ParameterizedType,
                                "Type info missing for OutputEmitter in %s.%s",
                                inspectType.getRawType().getName(), field.getName());

    // Extract the Output type from the first type argument of OutputEmitter.
    Type outputType = ((ParameterizedType) emitterType).getActualTypeArguments()[0];
    outputType = inspectType.resolveType(outputType).getType();
    String outputName = field.isAnnotationPresent(Output.class) ?
      field.getAnnotation(Output.class).value() : FlowletDefinition.DEFAULT_OUTPUT;

    Preconditions.checkArgument(Reflections.isResolved(outputType),
                                "Invalid type in %s.%s. Only Class or ParameterizedType are supported.",
                                inspectType.getRawType().getName(), field.getName());

    Preconditions.checkArgument(
      !outputTypes.containsKey(outputName),
      "Output with name '%s' already exists. Use @Output with different name; class: %s, field: %s",
      outputName, inspectType.getRawType().toString(), field.getName()
    );

    outputTypes.put(outputName, ImmutableSet.of(outputType));
  }
}
