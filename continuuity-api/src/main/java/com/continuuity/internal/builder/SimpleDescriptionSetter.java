package com.continuuity.internal.builder;

import com.continuuity.api.builder.DescriptionSetter;

/**
 * @param <T>
 */
public final class SimpleDescriptionSetter<T> implements DescriptionSetter<T> {

  private final Setter<String> setter;
  private final T next;

  public static <T> DescriptionSetter<T> create(Setter<String> setter, T next) {
    return new SimpleDescriptionSetter<T>(setter, next);
  }

  private SimpleDescriptionSetter(Setter<String> setter, T next) {
    this.setter = setter;
    this.next = next;
  }

  @Override
  public T setDescription(String description) {
    setter.set(description);
    return next;
  }
}
