package com.continuuity.api.mapreduce;

import com.continuuity.api.ProgramSpecification;
import com.continuuity.api.common.PropertyProvider;
import com.continuuity.internal.batch.DefaultMapReduceSpecification;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

/**
 * This class provides the specification for a MapReduce job. Create instances of this class via the {@link Builder} 
 * class by invoking the {@link Builder#with()} method.
 * <p>
 *   Example:
 * <pre>
 * {@code
 * MapReduceSpecification spec =
 *     new MapReduceSpecification.Builder.with()
 *       .setName("AggregateMetricsByTag")
 *       .setDescription("Aggregates metrics values by tag")
 *       .useInputDataSet("metricsTable")
 *       .useOutputDataSet("aggregatesTable")
 *       .build();
 * }
 * </pre>
 * </p>
 */
public interface MapReduceSpecification extends ProgramSpecification, PropertyProvider {

  /**
   * @return An immutable set of {@link com.continuuity.api.data.DataSet DataSets} that
   *         are used by the {@link MapReduce}.
   */
  Set<String> getDataSets();

  /**
   * @return name of the dataset to be used as output of mapreduce job or {@code null} if no dataset is used as output
   *         destination
   */
  String getOutputDataSet();

  /**
   * @return name The name of the dataset to be used as input to a MapReduce job or {@code null} 
   * if no dataset is used as the input source.
   */
  String getInputDataSet();

  /**
   * @return Amount of memory in MB specified for each mapper.
   */
  int getMapperMemoryMB();

  /**
   * @return Amount of memory in MB specified for each reducer.
   */
  int getReducerMemoryMB();

  /**
   * Builder for building {@link MapReduceSpecification}.
   */
  static final class Builder {
    private String name;
    private String description;
    private String inputDataSet;
    private String outputDataSet;
    private Map<String, String> arguments;
    private final ImmutableSet.Builder<String> dataSets = ImmutableSet.builder();
    private int mapperMemoryMB = 1024;
    private int reducerMemoryMB = 1024;

    /**
     * Start defining {@link MapReduceSpecification}.
     * @return An instance of {@link NameSetter}.
     */
    public static NameSetter with() {
      return new Builder().new NameSetter();
    }

    /**
     * Class for setting name.
     */
    public final class NameSetter {

      /**
       * Sets the name of the {@link MapReduce} job.
       * @param name Name of the MapReduce job.
       * @return Instance of this {@link Builder}.
       */
      public DescriptionSetter setName(String name) {
        Preconditions.checkArgument(name != null, "Name cannot be null.");
        Builder.this.name = name;
        return new DescriptionSetter();
      }
    }

    /**
     * Description setter for builder that guides you through the process of building
     * the specification.
     */
    public final class DescriptionSetter {

      /**
       * Sets the description for this {@link MapReduce} job.
       * @param description Description of the {@link MapReduce} job.
       * @return An instance of {@link AfterDescription}.
       */
      public AfterDescription setDescription(String description) {
        Preconditions.checkArgument(description != null, "Description cannot be null.");
        Builder.this.description = description;
        return new AfterDescription();
      }
    }

    /**
     * Part of builder for defining next steps after providing description.
     */
    public final class AfterDescription {

      /**
       * Adds the names of {@link com.continuuity.api.data.DataSet DataSets} used by the MapReduce job.
       *
       * @param dataSet DataSet name.
       * @param moreDataSets More DataSet names.
       * @return An instance of {@link AfterDescription}.
       */
      public AfterDescription useDataSet(String dataSet, String...moreDataSets) {
        dataSets.add(dataSet).add(moreDataSets);
        return this;
      }

      /**
       * Specifies which dataset to use as an input source for a MapReduce job. 
       * Automatically adds the dataset to the list of datasets used by this job, 
       * so no need to add the dataset with {@link #useDataSet(String, String...)} again.
       * <p>
       *   Usually, in this case whole dataset will be fed into mapreduce job. Alternatively, you can specify the
       *   dataset (and its data selection) to be fed into mapreduce job using
       *   {@link MapReduceContext#setInput(com.continuuity.api.data.batch.BatchReadable, java.util.List)} in
       *   {@link MapReduce#beforeSubmit(MapReduceContext)}.
       * </p>
       * @param dataSet Name of the dataset
       * @return an instance of {@link AfterDescription}
       */
      public AfterDescription useInputDataSet(String dataSet) {
        dataSets.add(dataSet);
        inputDataSet = dataSet;
        return this;
      }

      /**
       * Specifies which dataset to use as an output destination of mapreduce job. Automatically adds dataset to the
       * list of datasets used by this job. I.e. no need to add it with {@link #useDataSet(String, String...)} again.
       * @param dataSet Name of the dataset
       * @return an instance of {@link AfterDescription}
       */
      public AfterDescription useOutputDataSet(String dataSet) {
        dataSets.add(dataSet);
        outputDataSet = dataSet;
        return this;
      }

      /**
       * Adds a map of arguments that would be available to the mapreduce job
       * through the {@link MapReduceContext} at runtime.
       *
       * @param args The map of arguments.
       * @return An instance of {@link AfterDescription}.
       */
      public AfterDescription withArguments(Map<String, String> args) {
        arguments = ImmutableMap.copyOf(args);
        return this;
      }

      public AfterDescription setMapperMemoryMB(int memory) {
        mapperMemoryMB = memory;
        return this;
      }

      public AfterDescription setReducerMemoryMB(int memory) {
        reducerMemoryMB = memory;
        return this;
      }

      /**
       * @return build a {@link MapReduceSpecification}
       */
      public MapReduceSpecification build() {
        return new DefaultMapReduceSpecification(name, description, inputDataSet, outputDataSet,
                                                 dataSets.build(), arguments, mapperMemoryMB, reducerMemoryMB);
      }
    }

    private Builder() {}
  }
}
