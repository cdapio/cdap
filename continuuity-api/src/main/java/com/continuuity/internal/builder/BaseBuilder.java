package com.continuuity.internal.builder;

import com.continuuity.api.builder.Creator;

/**
 * @param <T>
 */
public abstract class BaseBuilder<T> implements Creator<T> {

  protected String name;
  protected String description;

  protected static Setter<String> getNameSetter(final BaseBuilder builder) {
    return new Setter<String>() {
      @Override
      public void set(String obj) {
        builder.name = obj;
      }
    };
  }

  protected static Setter<String> getDescriptionSetter(final BaseBuilder builder) {
    return new Setter<String>() {
      @Override
      public void set(String obj) {
        builder.description = obj;
      }
    };
  }
}
