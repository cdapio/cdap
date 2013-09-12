/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.workflow;

import com.continuuity.api.ProgramSpecification;
import com.continuuity.api.batch.MapReduce;
import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.api.builder.Creator;
import com.continuuity.api.builder.DescriptionSetter;
import com.continuuity.api.builder.NameSetter;
import com.continuuity.internal.batch.DefaultMapReduceSpecification;
import com.continuuity.internal.batch.ForwardingMapReduceSpecification;
import com.continuuity.internal.builder.BaseBuilder;
import com.continuuity.internal.builder.SimpleDescriptionSetter;
import com.continuuity.internal.builder.SimpleNameSetter;
import com.continuuity.internal.workflow.DefaultWorkflowActionSpecification;
import com.continuuity.internal.workflow.DefaultWorkflowSpecification;
import com.continuuity.internal.workflow.MapReduceWorkflowAction;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 *
 */
public interface WorkflowSpecification extends ProgramSpecification {

  List<WorkflowActionSpecification> getActions();

  Map<String, MapReduceSpecification> getMapReduces();

  /**
   *
   */
  final class Builder extends BaseBuilder<WorkflowSpecification> {

    private final List<WorkflowActionSpecification> actions = Lists.newArrayList();
    private final Map<String, MapReduceSpecification> mapReduces = Maps.newHashMap();

    /**
     *
     * @param <T>
     */
    public interface FirstAction<T> {

      T startWith(WorkflowAction action);

      T startWith(MapReduce mapReduce);
    }

    /**
     *
     * @param <T>
     */
    public interface MoreAction<T> {

      MoreAction<T> then(WorkflowAction action);

      MoreAction<T> then(MapReduce mapReduce);

      T last(WorkflowAction action);

      T last(MapReduce mapReduce);
    }

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
      return new DefaultWorkflowSpecification(name, description, actions);
    }

    /**
     * Adds a MapReduce to this workflow.
     * @param mapReduce
     * @return
     */
    private MapReduceSpecification addWorkflowMapReduce(MapReduce mapReduce) {
      MapReduceSpecification mapReduceSpec = new DefaultMapReduceSpecification(mapReduce);

      // Rename the map reduce based on the step in the workflow.
      final String mapReduceName = String.format("%s_%d", mapReduceSpec.getName(), actions.size());
      Preconditions.checkArgument(!mapReduces.containsKey(mapReduceName),
                                  "MapReduce %s already added for stage %s", mapReduceSpec.getName(), actions.size());

      mapReduceSpec = new ForwardingMapReduceSpecification(mapReduceSpec) {
        @Override
        public String getName() {
          return mapReduceName;
        }
      };

      // Add the map reduce to this workflow and also add the map reduce action.
      mapReduces.put(mapReduceName, mapReduceSpec);
      return mapReduceSpec;
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
        Preconditions.checkArgument(action != null, "WorkflowAction is null.");
        WorkflowActionSpecification spec = action.configure();
        builder.actions.add(new DefaultWorkflowActionSpecification(action.getClass().getName(), spec));
        return next;
      }

      @Override
      public T startWith(MapReduce mapReduce) {
        Preconditions.checkArgument(mapReduce != null, "MapReduce is null.");
        MapReduceSpecification mapReduceSpec = builder.addWorkflowMapReduce(mapReduce);
        return startWith(new MapReduceWorkflowAction(mapReduceSpec.getName()));
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
        Preconditions.checkArgument(action != null, "WorkflowAction is null.");
        WorkflowActionSpecification spec = action.configure();
        builder.actions.add(new DefaultWorkflowActionSpecification(action.getClass().getName(), spec));
        return this;
      }

      @Override
      public MoreAction<T> then(MapReduce mapReduce) {
        Preconditions.checkArgument(mapReduce != null, "MapReduce is null.");
        MapReduceSpecification mapReduceSpec = builder.addWorkflowMapReduce(mapReduce);
        return then(new MapReduceWorkflowAction(mapReduceSpec.getName()));
      }

      @Override
      public T last(WorkflowAction action) {
        then(action);
        return next;
      }

      @Override
      public T last(MapReduce mapReduce) {
        then(mapReduce);
        return next;
      }
    }

    private Builder() {
    }
  }
}
