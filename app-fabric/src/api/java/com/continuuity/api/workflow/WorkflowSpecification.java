/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.workflow;

import com.continuuity.api.builder.Creator;
import com.continuuity.api.builder.DescriptionSetter;
import com.continuuity.api.builder.NameSetter;

/**
 *
 */
public interface WorkFlowSpecification {

  String getName();

  String getDescription();

  /**
   *
   */
  final class Builder {

    private String name;
    private String description;

    public static NameSetter<
                    DescriptionSetter<
                      FirstTask<
                        MoreTask<
                          Creator<WorkFlowSpecification>
                        >
                      >
                    >
                  > with() {
      Builder builder = new Builder();
      return NameSetterImpl.create(builder,
        DescriptionSetterImpl.create(builder,
          FirstTaskImpl.create(builder,
            MoreTaskImpl.create(builder,
              CreatorImpl.create(builder)))));
    }

    private static final class NameSetterImpl<T> implements NameSetter<T> {

      private final Builder builder;
      private final T next;

      static <T> NameSetter<T> create(Builder builder, T next) {
        return new NameSetterImpl<T>(builder, next);
      }

      private NameSetterImpl(Builder builder, T next) {
        this.builder = builder;
        this.next = next;
      }

      @Override
      public T setName(String name) {
        builder.name = name;
        return next;
      }
    }

    private static final class DescriptionSetterImpl<T> implements DescriptionSetter<T> {

      private final Builder builder;
      private final T next;

      static <T> DescriptionSetter<T> create(Builder builder, T next) {
        return new DescriptionSetterImpl<T>(builder, next);
      }

      private DescriptionSetterImpl(Builder builder, T next) {
        this.builder = builder;
        this.next = next;
      }

      @Override
      public T setDescription(String description) {
        builder.description = description;
        return next;
      }
    }

    private static final class FirstTaskImpl<T> implements FirstTask<T> {

      private final Builder builder;
      private final T next;

      static <T> FirstTask<T> create(Builder builder, T next) {
        return new FirstTaskImpl<T>(builder, next);
      }

      private FirstTaskImpl(Builder builder, T next) {
        this.builder = builder;
        this.next = next;
      }

      @Override
      public T startWith(Runnable task) {
        return next;
      }
    }

    private static final class MoreTaskImpl<T> implements MoreTask<T> {

      private final Builder builder;
      private final T next;

      static <T> MoreTask<T> create(Builder builder, T next) {
        return new MoreTaskImpl<T>(builder, next);
      }

      private MoreTaskImpl(Builder builder, T next) {
        this.builder = builder;
        this.next = next;
      }

      @Override
      public MoreTask<T> then(Runnable task) {
        return this;
      }

      @Override
      public T last(Runnable task) {
        return next;
      }
    }

    private static final class CreatorImpl implements Creator<WorkFlowSpecification> {

      private final Builder builder;

      static Creator<WorkFlowSpecification> create(Builder builder) {
        return new CreatorImpl(builder);
      }

      private CreatorImpl(Builder builder) {
        this.builder = builder;
      }

      @Override
      public WorkFlowSpecification build() {
        return new WorkFlowSpecification() {
          @Override
          public String getName() {
            return builder.name;
          }

          @Override
          public String getDescription() {
            return builder.description;
          }
        };
      }
    }

    private Builder() {
    }
  }
}
