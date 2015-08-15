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
package co.cask.cdap.internal.specification;

import co.cask.cdap.api.annotation.Output;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.internal.lang.FieldVisitor;
import co.cask.cdap.internal.lang.Reflections;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashSet;
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
  public void visit(Object instance, Type inspectType, Type declareType, Field field) throws Exception {
    if (!OutputEmitter.class.equals(field.getType())) {
      return;
    }

    TypeToken<?> inspectTypeToken = TypeToken.of(inspectType);
    Type emitterType = inspectTypeToken.resolveType(field.getGenericType()).getType();
    if (!(emitterType instanceof ParameterizedType)) {
      throw new IllegalArgumentException(String.format("Type info missing for OutputEmitter in %s.%s",
                                                       inspectTypeToken.getRawType().getName(), field.getName()));
    }

    // Extract the Output type from the first type argument of OutputEmitter.
    Type outputType = ((ParameterizedType) emitterType).getActualTypeArguments()[0];
    outputType = inspectTypeToken.resolveType(outputType).getType();
    String outputName = field.isAnnotationPresent(Output.class) ?
      field.getAnnotation(Output.class).value() : FlowletDefinition.DEFAULT_OUTPUT;

    if (!Reflections.isResolved(outputType)) {
      throw new IllegalArgumentException(
        String.format("Invalid type in %s.%s. Only Class or ParameterizedType are supported.",
                      inspectTypeToken.getRawType().getName(), field.getName()));
    }

    if (outputTypes.containsKey(outputName)) {
      throw new IllegalArgumentException(
        String.format("Output with name '%s' already exists. Use @Output with different name; class: %s, field: %s",
                      outputName, inspectTypeToken.getRawType().toString(), field.getName()));
    }

    Set<Type> types = new HashSet<>();
    types.add(outputType);
    outputTypes.put(outputName, Collections.unmodifiableSet(types));
  }
}
