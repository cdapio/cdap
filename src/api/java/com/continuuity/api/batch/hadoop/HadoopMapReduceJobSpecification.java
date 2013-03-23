package com.continuuity.api.batch.hadoop;

import com.continuuity.internal.api.batch.hadoop.DefaultHadoopMapReduceJobSpecification;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

/**
 *
 */
public interface HadoopMapReduceJobSpecification {

  /**
   * @return Class name of the {@link HadoopMapReduceJob} class.
   */
  String getClassName();

  /**
   * @return Name of the {@link HadoopMapReduceJob}
   */
  String getName();

  /**
   * @return Description to be associated with {@link HadoopMapReduceJob}
   */
  String getDescription();

  /**
   * @return An immutable set of {@link com.continuuity.api.data.DataSet DataSets} that
   *         are used by the {@link HadoopMapReduceJob}.
   */
  Set<String> getDataSets();

  /**
   * @return An immutable map of arguments that was passed in when constructing the
   *         {@link HadoopMapReduceJobSpecification}.
   */
  Map<String, String> getArguments();

  /**
   * @return name of the dataset to be used as output of mapreduce job or {@code null} if no dataset is used as output
   *         destination
   */
  String getOutputDataSet();

  /**
   * @return name of the dataset to be used as input of mapreduce job or {@code null} if no dataset is used as input
   *         source
   */
  String getInputDataSet();

  /**
   * Builder for building {@link HadoopMapReduceJobSpecification}
   */
  static final class Builder {
    private String name;
    private String description;
    private String inputDataSet;
    private String outputDataSet;
    private Map<String, String> arguments;
    private final ImmutableSet.Builder<String> dataSets = ImmutableSet.builder();

    /**
     * Starts defining {@link HadoopMapReduceJobSpecification}
     * @return an instance of {@link NameSetter}
     */
    public static NameSetter with() {
      return new Builder().new NameSetter();
    }

    /**
     * Class for setting name.
     */
    public final class NameSetter {

      /**
       * Sets the name of the {@link HadoopMapReduceJob}
       * @param name of the mapreduce job.
       * @return instance of this {@link Builder}
       */
      public DescriptionSetter setName(String name) {
        Preconditions.checkArgument(name != null, "Name cannot be null.");
        Builder.this.name = name;
        return new DescriptionSetter();
      }
    }

    /**
     * Description setter for builder that guides you through process of building
     * the specification.
     */
    public final class DescriptionSetter {

      /**
       * Sets the description for this {@link HadoopMapReduceJob}
       * @param description of the {@link HadoopMapReduceJob}
       * @return An instance of {@link AfterDescription}
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
       * Adds the names of {@link com.continuuity.api.data.DataSet DataSets} used by the mapreduce job.
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
       * Specifies which dataset to use as an input source for mapreduce job. Automatically adds dataset to the list of
       * datasets used by this job. I.e. no need to add it with {@link #useDataSet(String, String...)} again.
       * <p>
       *   Usually, in this case whole dataset will be fed into mapreduce job. Alternatively, you can specify the
       *   dataset (and its data selection) to be fed into mapreduce job using
       *   {@link HadoopMapReduceJobContext#setInput(com.continuuity.api.data.batch.BatchReadable, java.util.List)} in
       *   {@link HadoopMapReduceJob#beforeSubmit(HadoopMapReduceJobContext)}.
       * </p>
       * @param dataSet name of the dataset
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
       * @param dataSet name of the dataset
       * @return an instance of {@link AfterDescription}
       */
      public AfterDescription useOutputDataSet(String dataSet) {
        dataSets.add(dataSet);
        outputDataSet = dataSet;
        return this;
      }

      /**
       * Adds a map of arguments that would be available to the mapreduce job
       * through the {@link HadoopMapReduceJobContext} at runtime.
       *
       * @param args The map of arguments.
       * @return An instance of {@link AfterDescription}.
       */
      public AfterDescription withArguments(Map<String, String> args) {
        arguments = ImmutableMap.copyOf(args);
        return this;
      }

      /**
       * @return build a {@link HadoopMapReduceJobSpecification}
       */
      public HadoopMapReduceJobSpecification build() {
        return new DefaultHadoopMapReduceJobSpecification(name, description, inputDataSet, outputDataSet,
                                                    dataSets.build(), arguments);
      }
    }

    private Builder() {}
  }
}
