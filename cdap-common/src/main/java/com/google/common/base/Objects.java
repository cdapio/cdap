/*
 * Copyright (C) 2007 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.common.base;

import java.util.Arrays;
import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

/**
 * Helper functions that operate on any {@code Object}.
 *
 * <p>Includes backwards-compatibility methods (like {@code toStringHelper}) for Guava 13.
 */
public final class Objects extends ExtraObjectsMethodsForWeb {

  private Objects() {
  }

  /**
   * Determines whether two possibly-null objects are equal. Returns:
   *
   * <ul>
   *   <li>{@code true} if {@code a} and {@code b} are both null.
   *   <li>{@code true} if {@code a} and {@code b} are both non-null and they are equal according to
   *       {@link Object#equals(Object)}.
   *   <li>{@code false} in all other situations.
   * </ul>
   */
  public static boolean equal(@CheckForNull Object a, @CheckForNull Object b) {
    return a == b || (a != null && a.equals(b));
  }

  /**
   * Generates a hash code for multiple values.
   */
  public static int hashCode(@CheckForNull Object... objects) {
    return Arrays.hashCode(objects);
  }

  /**
   * Guava 13 backwards-compatibility method.
   *
   * @param first the first value
   * @param second the second value
   * @param <T> the type
   * @return first if non-null, else second
   */
  public static <T> T firstNonNull(@Nullable T first, @Nullable T second) {
    return MoreObjects.firstNonNull(first, second);
  }

  /**
   * Guava 13 backwards-compatibility method.
   *
   * @param self the object to generate the string for
   * @return a {@link ToStringHelper}
   */
  public static ToStringHelper toStringHelper(Object self) {
    return new ToStringHelper(MoreObjects.toStringHelper(self));
  }

  /**
   * Guava 13 backwards-compatibility method.
   *
   * @param clazz the class to generate the string for
   * @return a {@link ToStringHelper}
   */
  public static ToStringHelper toStringHelper(Class<?> clazz) {
    return new ToStringHelper(MoreObjects.toStringHelper(clazz));
  }

  /**
   * Guava 13 backwards-compatibility method.
   *
   * @param className the class name to generate the string for
   * @return a {@link ToStringHelper}
   */
  public static ToStringHelper toStringHelper(String className) {
    return new ToStringHelper(MoreObjects.toStringHelper(className));
  }

  /**
   * Support class for {@link Objects#toStringHelper}.
   */
  public static final class ToStringHelper {
    private final MoreObjects.ToStringHelper delegate;

    ToStringHelper(MoreObjects.ToStringHelper delegate) {
      this.delegate = delegate;
    }

    public ToStringHelper omitNullValues() {
      delegate.omitNullValues();
      return this;
    }

    public ToStringHelper add(String name, @Nullable Object value) {
      delegate.add(name, value);
      return this;
    }

    public ToStringHelper add(String name, boolean value) {
      delegate.add(name, value);
      return this;
    }

    public ToStringHelper add(String name, char value) {
      delegate.add(name, value);
      return this;
    }

    public ToStringHelper add(String name, double value) {
      delegate.add(name, value);
      return this;
    }

    public ToStringHelper add(String name, float value) {
      delegate.add(name, value);
      return this;
    }

    public ToStringHelper add(String name, int value) {
      delegate.add(name, value);
      return this;
    }

    public ToStringHelper add(String name, long value) {
      delegate.add(name, value);
      return this;
    }

    public ToStringHelper addValue(@Nullable Object value) {
      delegate.addValue(value);
      return this;
    }

    public ToStringHelper addValue(boolean value) {
      delegate.addValue(value);
      return this;
    }

    public ToStringHelper addValue(char value) {
      delegate.addValue(value);
      return this;
    }

    public ToStringHelper addValue(double value) {
      delegate.addValue(value);
      return this;
    }

    public ToStringHelper addValue(float value) {
      delegate.addValue(value);
      return this;
    }

    public ToStringHelper addValue(int value) {
      delegate.addValue(value);
      return this;
    }

    public ToStringHelper addValue(long value) {
      delegate.addValue(value);
      return this;
    }

    @Override
    public String toString() {
      return delegate.toString();
    }
  }
}
