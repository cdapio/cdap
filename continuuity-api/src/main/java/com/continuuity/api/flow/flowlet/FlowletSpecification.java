/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow.flowlet;

import com.continuuity.api.ResourceSpecification;
import com.continuuity.api.common.PropertyProvider;
import com.continuuity.internal.flowlet.DefaultFlowletSpecification;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

/**
 * This class provides specification of a Flowlet. Instance of this class should be created through
 * the {@link Builder} class by invoking the {@link Builder#with()} method.
 *
 * <pre>
 * {@code
 * FlowletSpecification flowletSpecification =
 *  FlowletSpecification flowletSpecification =
 *      FlowletSpecification.Builder.with()
 *        .setName("tokenCount")
 *        .setDescription("Token counting flow")
 *        .setFailurePolicy(FailurePolicy.RETRY)
 *        .build();
 * }
 * </pre>
 */
public interface FlowletSpecification extends PropertyProvider {

  /**
   * @return Class name of the {@link Flowlet} class.
   */
  String getClassName();

  /**
   * @return Name of the flowlet.
   */
  String getName();

  /**
   * @return Description of the flowlet.
   */
  String getDescription();

  /**
   * @return The failure policy of the flowlet.
   */
  FailurePolicy getFailurePolicy();

  /**
   * @return An immutable set of {@link com.continuuity.api.data.DataSet DataSets} name that
   *         used by the {@link Flowlet}.
   */
  Set<String> getDataSets();

  /**
   * @return The {@link ResourceSpecification} for the flowlet.
   */
  ResourceSpecification getResources();

  /**
   * Builder for creating instance of {@link FlowletSpecification}. The builder instance is
   * not reusable, meaning each instance of this class can only be used to create one instance
   * of {@link FlowletSpecification}.
   */
  static final class Builder {

    private String name;
    private String description;
    private FailurePolicy failurePolicy = FailurePolicy.RETRY;
    private final ImmutableSet.Builder<String> dataSets = ImmutableSet.builder();
    private Map<String, String> arguments;
    private ResourceSpecification resources = ResourceSpecification.BASIC;

    /**
     * Creates a {@link Builder} for building instance of this class.
     *
     * @return A new builder instance.
     */
    public static NameSetter with() {
      return new Builder().new NameSetter();
    }

    public final class NameSetter {
      /**
       * Sets the name of a flowlet.
       * @param name Name of the flowlet.
       * @return An instance of {@link DescriptionSetter}
       */
      public DescriptionSetter setName(String name) {
        Preconditions.checkArgument(name != null, "Name cannot be null.");
        Builder.this.name = name;
        return new DescriptionSetter();
      }
    }

    /**
     * Class defining the description setter that is used as part of the builder.
     */
    public final class DescriptionSetter {
      /**
       * Sets the description of the flowlet.
       * @param description Descripition to be associated with flowlet.
       * @return An instance of what needs to be done after description {@link AfterDescription}
       */
      public AfterDescription setDescription(String description) {
        Preconditions.checkArgument(description != null, "Description cannot be null.");
        Builder.this.description = description;
        return new AfterDescription();
      }
    }

    /**
     * Class defining the action after defining the description for a flowlet.
     */
    public final class AfterDescription {

      /**
       * Sets the failure policy of a flowlet.
       * @param policy Policy to be associated with a flowlet for handling processing failures.
       * @return An instance of {@link AfterDescription}
       */
      public AfterDescription setFailurePolicy(FailurePolicy policy) {
        Preconditions.checkArgument(policy != null, "FailurePolicy cannot be null");
        failurePolicy = policy;
        return this;
      }

      /**
       * Adds the names of {@link com.continuuity.api.data.DataSet DataSets} used by the flowlet.
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
       * Adds a map of arguments that would be available to the flowlet through the {@link FlowletContext} at runtime.
       *
       * @param args The map of arguments.
       * @return An instance of {@link AfterDescription}.
       */
      public AfterDescription withArguments(Map<String, String> args) {
        arguments = ImmutableMap.copyOf(args);
        return this;
      }

      public AfterDescription withResources(ResourceSpecification resourceSpec) {
        Preconditions.checkArgument(resourceSpec != null, "Resources cannot be null.");
        resources = resourceSpec;
        return this;
      }

      /**
       * Creates an instance of {@link FlowletSpecification}.
       * @return An instance of {@link FlowletSpecification}.
       */
      public FlowletSpecification build() {
        return new DefaultFlowletSpecification(name, description, failurePolicy,
                                               dataSets.build(), arguments, resources);
      }
    }

    /**
     * Private builder to maintain builder contract.
     */
    private Builder() {
    }
  }
}
