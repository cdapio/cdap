/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.specification;

import com.continuuity.api.annotation.Batch;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.Tick;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.internal.lang.MethodVisitor;
import com.continuuity.internal.lang.Reflections;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public final class ProcessMethodExtractor extends MethodVisitor {

  private final Map<String, Set<Type>> inputTypes;
  private final Set<FlowletMethod> seenMethods;

  public ProcessMethodExtractor(Map<String, Set<Type>> inputTypes) {
    this.inputTypes = inputTypes;
    this.seenMethods = Sets.newHashSet();
  }

  @Override
  public void visit(Object instance, TypeToken<?> inspectType,
                    TypeToken<?> declareType, Method method) throws Exception {

    if (!seenMethods.add(new FlowletMethod(method, inspectType))) {
      // The method is already seen. It can only happen if a children class override a parent class method and
      // is visting the parent method, since the method visiting order is always from the leaf class walking
      // up the class hierarchy.
      return;
    }

    ProcessInput processInputAnnotation = method.getAnnotation(ProcessInput.class);
    Tick tickAnnotation = method.getAnnotation(Tick.class);

    if (processInputAnnotation == null && tickAnnotation == null) {
      return;
    }

    // Check for tick method
    if (tickAnnotation != null) {
      Preconditions.checkArgument(processInputAnnotation == null,
                                  "Tick method %s.%s should not have ProcessInput.",
                                  inspectType.getRawType().getName(), method);
      Preconditions.checkArgument(method.getParameterTypes().length == 0,
                                  "Tick method %s.%s cannot have parameters.",
                                  inspectType.getRawType().getName(), method);
      return;
    }

    Type[] methodParams = method.getGenericParameterTypes();
    Preconditions.checkArgument(methodParams.length > 0 && methodParams.length <= 2,
                                "Parameter missing from process method %s.%s.",
                                inspectType.getRawType().getName(), method);

    // If there is more than one parameter there can only be exactly two; the second one must be InputContext type
    if (methodParams.length == 2) {
      Preconditions.checkArgument(InputContext.class.equals(TypeToken.of(methodParams[1]).getRawType()),
                                  "Second parameter must be InputContext type for process method %s.%s.",
                                  inspectType.getRawType().getName(), method);
    }

    // Extract the Input type from the first parameter of the process method
    Type inputType = getInputType(inspectType, method, inspectType.resolveType(methodParams[0]).getType());
    Preconditions.checkArgument(Reflections.isResolved(inputType),
                                "Invalid type in %s.%s. Only Class or ParameterizedType are supported.",
                                inspectType.getRawType().getName(), method);

    List<String> inputNames = Lists.newLinkedList();
    if (processInputAnnotation.value().length == 0) {
      inputNames.add(FlowletDefinition.ANY_INPUT);
    } else {
      Collections.addAll(inputNames, processInputAnnotation.value());
    }

    for (String inputName : inputNames) {
      Set<Type> types = inputTypes.get(inputName);
      if (types == null) {
        types = Sets.newHashSet();
        inputTypes.put(inputName, types);
      }
      Preconditions.checkArgument(types.add(inputType),
                                  "Same type already defined for the same input name %s in process method %s.%s.",
                                  inputName, inspectType.getRawType().getName(), method);
    }
  }

  private Type getInputType(TypeToken<?> type, Method method, Type methodParam) {
    // In batch mode, if the first parameter is an iterator then extract the type information from
    // the iterator's type parameter
    if (method.getAnnotation(Batch.class) != null) {
      if (methodParam instanceof ParameterizedType) {
        ParameterizedType pType = (ParameterizedType) methodParam;
        if (pType.getRawType().equals(Iterator.class)) {
          methodParam = pType.getActualTypeArguments()[0];
        }
      }
    } else {
      // Check to see if there is an method param which is a type of iterator.
      // This check is needed because we don't support type projection with iterator.
      if (methodParam instanceof ParameterizedType) {
        ParameterizedType pType = (ParameterizedType) methodParam;
        Preconditions.checkArgument(!pType.getRawType().equals(Iterator.class),
                                    "Iterator type should only be used with Batch annotation for process method %s.%s",
                                    type.getRawType().getName(), method.getName());
      }
    }
    return methodParam;
  }
}
