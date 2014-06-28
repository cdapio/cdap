package com.continuuity.internal.builder;

import com.continuuity.api.builder.NameSetter;

/**
 * @param <T>
 */
public final class SimpleNameSetter<T> implements NameSetter<T> {

  private final Setter<String> setter;
  private final T next;

  public static <T> NameSetter<T> create(Setter<String> setter, T next) {
    return new SimpleNameSetter<T>(setter, next);
  }

  private SimpleNameSetter(Setter<String> setter, T next) {
    this.setter = setter;
    this.next = next;
  }

  @Override
  public T setName(String name) {
    setter.set(name);
    return next;
  }
}
