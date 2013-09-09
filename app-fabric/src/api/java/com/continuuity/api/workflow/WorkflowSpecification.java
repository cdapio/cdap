/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.workflow;

import com.continuuity.api.builder.Creator;
import com.continuuity.api.builder.DescriptionSetter;
import com.continuuity.api.builder.NameSetter;
import com.continuuity.internal.builder.BaseBuilder;
import com.continuuity.internal.builder.SimpleDescriptionSetter;
import com.continuuity.internal.builder.SimpleNameSetter;

/**
 *
 */
public interface WorkflowSpecification {

  String getName();

  String getDescription();


  /**
   *
   * @param <T>
   */
  interface FirstAction<T> {

    T startWith(WorkflowAction action);
  }

  /**
   *
   * @param <T>
   */
  interface MoreAction<T> {

    MoreAction<T> then(WorkflowAction action);

    T last(WorkflowAction action);
  }

  /**
   *
   */
  final class Builder extends BaseBuilder<WorkflowSpecification> {

    public static NameSetter<DescriptionSetter<FirstAction<MoreAction<Creator<WorkflowSpecification>>>>> with() {
      Builder builder = new Builder();

      return SimpleNameSetter.create(
        getNameSetter(builder), SimpleDescriptionSetter.create(
        getDescriptionSetter(builder), FirstActionImpl.create(
        builder, MoreActionImpl.create(
        builder, (Creator<WorkflowSpecification>) builder))));
    }

    @Override
    public WorkflowSpecification build() {
      return new WorkflowSpecification() {
        @Override
        public String getName() {
          return name;
        }

        @Override
        public String getDescription() {
          return description;
        }
      };
    }

    private static final class FirstActionImpl<T> implements FirstAction<T> {

      private final Builder builder;
      private final T next;

      static <T> FirstAction<T> create(Builder builder, T next) {
        return new FirstActionImpl<T>(builder, next);
      }

      private FirstActionImpl(Builder builder, T next) {
        this.builder = builder;
        this.next = next;
      }

      @Override
      public T startWith(WorkflowAction action) {
        return next;
      }
    }

    private static final class MoreActionImpl<T> implements MoreAction<T> {

      private final Builder builder;
      private final T next;

      static <T> MoreAction<T> create(Builder builder, T next) {
        return new MoreActionImpl<T>(builder, next);
      }

      private MoreActionImpl(Builder builder, T next) {
        this.builder = builder;
        this.next = next;
      }

      @Override
      public MoreAction<T> then(WorkflowAction action) {
        return this;
      }

      @Override
      public T last(WorkflowAction action) {
        return next;
      }
    }

    private Builder() {
    }
  }
}
