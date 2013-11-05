/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.workflow;

import com.continuuity.api.builder.Creator;
import com.continuuity.api.builder.DescriptionSetter;
import com.continuuity.api.builder.NameSetter;
import com.continuuity.api.mapreduce.MapReduce;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.schedule.SchedulableProgramSpecification;
import com.continuuity.api.schedule.Schedule;
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
 * Specification for a {@link Workflow} -- an instance of this class is created by the {@link Builder} class.
 *
 * <p>
 * Example WorkflowSpecification for a scheduled workflow:
 *
 *  <pre>
 *    <code>
 *      {@literal @}Override
 *      public WorkflowSpecification configure() {
 *        return WorkflowSpecification.Builder.with()
 *        .setName("PurchaseHistoryWorkflow")
 *        .setDescription("PurchaseHistoryWorkflow description")
 *        .onlyWith(new PurchaseHistoryBuilder())
 *        .addSchedule(new Schedule("DailySchedule", "Run every day at 4:00 A.M.", "0 4 * * *", 
 *                     Schedule.Action.START))
 *        .build();
 *      }
 *    </code>
 *  </pre>
 * 
 * See the Purchase example application.
 */
public interface WorkflowSpecification extends SchedulableProgramSpecification {

  List<WorkflowActionSpecification> getActions();

  Map<String, MapReduceSpecification> getMapReduce();

  /**
   * Builder for adding the first action to the workflow.
   * @param <T> Type of the next builder object.
   */
  public interface FirstAction<T> {

    MoreAction<T> startWith(WorkflowAction action);

    MoreAction<T> startWith(MapReduce mapReduce);

    T onlyWith(WorkflowAction action);

    T onlyWith(MapReduce mapReduce);
  }

  /**
   * Builder for adding more actions to the workflow.
   * @param <T> Type of the next builder object.
   */
  public interface MoreAction<T> {

    MoreAction<T> then(WorkflowAction action);

    MoreAction<T> then(MapReduce mapReduce);

    T last(WorkflowAction action);

    T last(MapReduce mapReduce);
  }

  /**
   * Builder for setting up the schedule of the workflow.
   * @param <T> Type of the next builder object.
   */
  public interface ScheduleSetter<T> {
    T addSchedule(Schedule schedule);
  }

  /**
   * Builder interface for the last stage of building the {@link WorkflowSpecification}.
   */
  interface SpecificationCreator extends Creator<WorkflowSpecification>,
                                         ScheduleSetter<SpecificationCreator> { }

  /**
   * Builder class for constructing the {@link WorkflowSpecification}.
   */
  final class Builder extends BaseBuilder<WorkflowSpecification> implements SpecificationCreator {

    private final List<WorkflowActionSpecification> actions = Lists.newArrayList();
    private final Map<String, MapReduceSpecification> mapReduces = Maps.newHashMap();
    private final List<Schedule> schedules = Lists.newArrayList();

    /**
     * Returns an instance of builder.
     */
    public static NameSetter<DescriptionSetter<FirstAction<SpecificationCreator>>> with() {
      Builder builder = new Builder();

      return SimpleNameSetter.create(
        getNameSetter(builder), SimpleDescriptionSetter.create(
        getDescriptionSetter(builder), FirstActionImpl.create(
        builder, (SpecificationCreator) builder)));
    }

    @Override
    public WorkflowSpecification build() {
      return new DefaultWorkflowSpecification(name, description, actions, mapReduces, schedules);
    }

    /**
     * Adds a {@link MapReduce} job to this workflow.
     * @param mapReduce The map reduce job to add.
     * @return A {@link MapReduceSpecification} used for the given MapReduce job.
     */
    private MapReduceSpecification addWorkflowMapReduce(MapReduce mapReduce) {
      MapReduceSpecification mapReduceSpec = new DefaultMapReduceSpecification(mapReduce);

      // Rename the MapReduce job based on the step in the workflow.
      final String mapReduceName = String.format("%s_%s", name, mapReduceSpec.getName());
      mapReduceSpec = new ForwardingMapReduceSpecification(mapReduceSpec) {
        @Override
        public String getName() {
          return mapReduceName;
        }
      };

      // Add the MapReduce job and the MapReduce actionto this workflow.
      mapReduces.put(mapReduceName, mapReduceSpec);
      return mapReduceSpec;
    }


    @Override
    public SpecificationCreator addSchedule(Schedule schedule) {
      schedules.add(schedule);
      return this;
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
      public MoreAction<T> startWith(WorkflowAction action) {
        Preconditions.checkArgument(action != null, "WorkflowAction is null.");
        builder.actions.add(new DefaultWorkflowActionSpecification(action));
        return new MoreActionImpl<T>(builder, next);
      }

      @Override
      public MoreAction<T> startWith(MapReduce mapReduce) {
        Preconditions.checkArgument(mapReduce != null, "MapReduce is null.");
        MapReduceSpecification mapReduceSpec = builder.addWorkflowMapReduce(mapReduce);
        return startWith(new MapReduceWorkflowAction(mapReduce.configure().getName(), mapReduceSpec.getName()));
      }

      @Override
      public T onlyWith(WorkflowAction action) {
        Preconditions.checkArgument(action != null, "WorkflowAction is null.");
        builder.actions.add(new DefaultWorkflowActionSpecification(action));
        return next;
      }

      @Override
      public T onlyWith(MapReduce mapReduce) {
        Preconditions.checkArgument(mapReduce != null, "MapReduce is null.");
        MapReduceSpecification mapReduceSpec = builder.addWorkflowMapReduce(mapReduce);
        return onlyWith(new MapReduceWorkflowAction(mapReduce.configure().getName(), mapReduceSpec.getName()));
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
        builder.actions.add(new DefaultWorkflowActionSpecification(action));
        return this;
      }

      @Override
      public MoreAction<T> then(MapReduce mapReduce) {
        Preconditions.checkArgument(mapReduce != null, "MapReduce is null.");
        MapReduceSpecification mapReduceSpec = builder.addWorkflowMapReduce(mapReduce);
        return then(new MapReduceWorkflowAction(mapReduce.configure().getName(), mapReduceSpec.getName()));
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
