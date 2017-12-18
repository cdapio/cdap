/*
 * Copyright © 2016-2017 Cask Data, Inc.
 * Portions copyright (C) 2012 The Guava Authors
 * Derived from the Google Guava Project
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

package co.cask.cdap.internal.guava.reflect;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Represents a method or constructor parameter.
 */
public final class Parameter implements AnnotatedElement {

  private final Invokable<?, ?> declaration;
  private final int position;
  private final TypeToken<?> type;
  private final List<Annotation> annotations;

  Parameter(
      Invokable<?, ?> declaration,
      int position,
      TypeToken<?> type,
      Annotation[] annotations) {
    this.declaration = declaration;
    this.position = position;
    this.type = type;
    this.annotations = Arrays.asList(annotations);
  }

  /** Returns the type of the parameter. */
  public TypeToken<?> getType() {
    return type;
  }

  /** Returns the {@link Invokable} that declares this parameter. */
  public Invokable<?, ?> getDeclaringInvokable() {
    return declaration;
  }

  @Override public boolean isAnnotationPresent(Class<? extends Annotation> annotationType) {
    return getAnnotation(annotationType) != null;
  }

  @Override
  @Nullable
  public <A extends Annotation> A getAnnotation(Class<A> annotationType) {
    Preconditions.checkNotNull(annotationType);
    for (Annotation annotation : annotations) {
      if (annotationType.isInstance(annotation)) {
        return annotationType.cast(annotation);
      }
    }
    return null;
  }

  @Override public Annotation[] getAnnotations() {
    return getDeclaredAnnotations();
  }

  /**
   * @since 18.0
   */
  // @Override on JDK8
  @Override
  public <A extends Annotation> A[] getAnnotationsByType(Class<A> annotationType) {
    return getDeclaredAnnotationsByType(annotationType);
  }

  /**
   * @since 18.0
   */
  // @Override on JDK8
  @Override public Annotation[] getDeclaredAnnotations() {
    return annotations.toArray(new Annotation[annotations.size()]);
  }

  /**
   * @since 18.0
   */
  // @Override on JDK8
  @Override
  @Nullable
  public <A extends Annotation> A getDeclaredAnnotation(Class<A> annotationType) {
    Preconditions.checkNotNull(annotationType);
    Iterator<A> itor = Iterables.filter(annotations, annotationType).iterator();
    return itor.hasNext() ? itor.next() : null;
  }

  /**
   * @since 18.0
   */
  // @Override on JDK8
  @Override
  public <A extends Annotation> A[] getDeclaredAnnotationsByType(Class<A> annotationType) {
    List<A> list = Iterables.addAll(Iterables.filter(annotations, annotationType), new ArrayList<A>());
    @SuppressWarnings("unchecked")
    A[] array = (A[]) Array.newInstance(annotationType, list.size());
    return list.toArray(array);
  }

  @Override public boolean equals(@Nullable Object obj) {
    if (obj instanceof Parameter) {
      Parameter that = (Parameter) obj;
      return position == that.position && declaration.equals(that.declaration);
    }
    return false;
  }

  @Override public int hashCode() {
    return position;
  }

  @Override public String toString() {
    return type + " arg" + position;
  }
}
