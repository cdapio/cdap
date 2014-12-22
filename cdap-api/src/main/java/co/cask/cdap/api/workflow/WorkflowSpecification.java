/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.api.workflow;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.builder.Creator;
import co.cask.cdap.api.builder.DescriptionSetter;
import co.cask.cdap.api.builder.NameSetter;
import co.cask.cdap.api.mapreduce.MapReduce;
import co.cask.cdap.api.mapreduce.MapReduceConfigurer;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.schedule.SchedulableProgramSpecification;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkConfigurer;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.internal.batch.DefaultMapReduceSpecification;
import co.cask.cdap.internal.batch.ForwardingMapReduceSpecification;
import co.cask.cdap.internal.builder.BaseBuilder;
import co.cask.cdap.internal.builder.SimpleDescriptionSetter;
import co.cask.cdap.internal.builder.SimpleNameSetter;
import co.cask.cdap.internal.workflow.DefaultWorkflowActionSpecification;
import co.cask.cdap.internal.workflow.DefaultWorkflowSpecification;
import co.cask.cdap.internal.workflow.ProgramWorkflowAction;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  Map<String, SparkSpecification> getSparks();

  /**
   * Builder for adding the first action to the workflow.
   * @param <T> Type of the next builder object.
   */
  public interface FirstAction<T> {

    MoreAction<T> startWith(WorkflowAction action);

    MoreAction<T> startWith(MapReduce mapReduce);

    MoreAction<T> startWith(Spark spark);

    T onlyWith(WorkflowAction action);

    T onlyWith(MapReduce mapReduce);

    T onlyWith(Spark spark);
  }

  /**
   * Builder for adding more actions to the workflow.
   * @param <T> Type of the next builder object.
   */
  public interface MoreAction<T> {

    MoreAction<T> then(WorkflowAction action);

    MoreAction<T> then(MapReduce mapReduce);

    MoreAction<T> then(Spark spark);

    T last(WorkflowAction action);

    T last(MapReduce mapReduce);

    T last(Spark spark);
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
    private final Map<String, SparkSpecification> sparks = Maps.newHashMap();
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
      return new DefaultWorkflowSpecification(name, description, actions, mapReduces, sparks, schedules);
    }

    /**
     * Adds a {@link MapReduce} job to this workflow.
     * @param mapReduce The map reduce job to add.
     * @return A {@link MapReduceSpecification} used for the given MapReduce job.
     */
    private MapReduceSpecification addWorkflowMapReduce(MapReduce mapReduce) {
      WorkflowMapReduceConfigurer configurer = new WorkflowMapReduceConfigurer(mapReduce);
      mapReduce.configure(configurer);
      MapReduceSpecification mapReduceSpec = configurer.createSpecification();

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

    /**
     * Adds a {@link Spark} job to this workflow.
     *
     * @param spark The Spark job to add.
     * @return A {@link SparkSpecification} used for the given Spark job.
     */
    private SparkSpecification addWorkflowSpark(Spark spark) {
      WorkflowSparkConfigurer configurer = new WorkflowSparkConfigurer(spark);
      spark.configure(configurer);
      SparkSpecification sparkSpec = configurer.createSpecification();

      // Rename the spark job based on the step in the workflow.
      final String sparkName = String.format("%s_%s", name, sparkSpec.getName());
      sparkSpec = new SparkSpecification(sparkSpec.getClassName(), sparkName, sparkSpec.getDescription(),
                                         sparkSpec.getMainClassName(), sparkSpec.getProperties());

      // Add the spark job and the spark action to this workflow.
      sparks.put(sparkName, sparkSpec);
      return sparkSpec;
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
        return startWith(new ProgramWorkflowAction(mapReduceSpec.getName(), mapReduceSpec.getName(),
                                                   WorkflowSupportedProgram.MAPREDUCE));
      }

      @Override
      public MoreAction<T> startWith(Spark spark) {
        Preconditions.checkArgument(spark != null, "Spark is null.");
        SparkSpecification sparkSpec = builder.addWorkflowSpark(spark);
        return startWith(new ProgramWorkflowAction(sparkSpec.getName(), sparkSpec.getName(),
                                                   WorkflowSupportedProgram.SPARK));
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
        return onlyWith(new ProgramWorkflowAction(mapReduceSpec.getName(), mapReduceSpec.getName(),
                                                  WorkflowSupportedProgram.MAPREDUCE));
      }

      @Override
      public T onlyWith(Spark spark) {
        Preconditions.checkArgument(spark != null, "Spark is null.");
        SparkSpecification sparkSpec = builder.addWorkflowSpark(spark);
        return onlyWith(new ProgramWorkflowAction(sparkSpec.getName(), sparkSpec.getName(),
                                                  WorkflowSupportedProgram.SPARK));
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
        return then(new ProgramWorkflowAction(mapReduceSpec.getName(), mapReduceSpec.getName(),
                                              WorkflowSupportedProgram.MAPREDUCE));
      }

      @Override
      public MoreAction<T> then(Spark spark) {
        Preconditions.checkArgument(spark != null, "Spark is null.");
        SparkSpecification sparkSpec = builder.addWorkflowSpark(spark);
        return then(new ProgramWorkflowAction(sparkSpec.getName(), sparkSpec.getName(),
                                              WorkflowSupportedProgram.SPARK));
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

      @Override
      public T last(Spark spark) {
        then(spark);
        return next;
      }
    }

    private Builder() {
    }

    // TODO (CDAP-450): Temporary. Remove when move workflow to use Configurer to configure
    private static final class WorkflowMapReduceConfigurer implements MapReduceConfigurer {
      private final String className;
      private String name;
      private String description;
      private Map<String, String> properties;
      private Set<String> datasets;
      private String inputDataset;
      private String outputDataset;
      private Resources mapperResources;
      private Resources reducerResources;

      public WorkflowMapReduceConfigurer(MapReduce mapReduce) {
        this.className = mapReduce.getClass().getName();
        this.name = mapReduce.getClass().getSimpleName();
        this.description = "";
        this.datasets = ImmutableSet.of();
      }

      @Override
      public void setName(String name) {
        this.name = name;
      }

      @Override
      public void setDescription(String description) {
        this.description = description;
      }

      @Override
      public void setProperties(Map<String, String> properties) {
        this.properties = ImmutableMap.copyOf(properties);
      }

      @Override
      public void useDatasets(Iterable<String> datasets) {
        this.datasets = ImmutableSet.copyOf(datasets);
      }

      @Override
      public void setInputDataset(String dataset) {
        this.inputDataset = dataset;
      }

      @Override
      public void setOutputDataset(String dataset) {
        this.outputDataset = dataset;
      }

      @Override
      public void setMapperResources(Resources resources) {
        this.mapperResources = resources;
      }

      @Override
      public void setReducerResources(Resources resources) {
        this.reducerResources = resources;
      }

      MapReduceSpecification createSpecification() {
        return new DefaultMapReduceSpecification(className, name, description, inputDataset, outputDataset, datasets,
                                                 properties, mapperResources, reducerResources);
      }
    }

    // TODO (CDAP-450): Update when Workflow is changed to use Configurer for its configuration
    private static final class WorkflowSparkConfigurer implements SparkConfigurer {
      private final String className;
      private String name;
      private String description;
      private String mainClassName;
      private Map<String, String> properties;

      public WorkflowSparkConfigurer(Spark spark) {
        this.className = spark.getClass().getName();
        this.name = spark.getClass().getSimpleName();
        this.description = "";
        this.properties = Collections.emptyMap();
      }

      @Override
      public void setName(String name) {
        this.name = name;
      }

      @Override
      public void setDescription(String description) {
        this.description = description;
      }

      @Override
      public void setMainClassName(String mainClassName) {
        this.mainClassName = mainClassName;
      }

      @Override
      public void setProperties(Map<String, String> properties) {
        this.properties = ImmutableMap.copyOf(properties);
      }

      SparkSpecification createSpecification() {
        return new SparkSpecification(className, name, description, mainClassName, properties);
      }
    }
  }
}
