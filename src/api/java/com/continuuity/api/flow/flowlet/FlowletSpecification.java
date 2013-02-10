/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow.flowlet;

import com.google.common.base.Preconditions;

/**
 * This class provides specification of a Flowlet. Instance of this class should be created through
 * the {@link Builder} class by invoking the {@link #builder()} method.
 *
 * <pre>
 * {@code
 * FlowletSpecification flowletSpecification =
 *  FlowletSpecification flowletSpecification =
 *      FlowletSpecification.builder()
 *        .setName("tokenCount")
 *        .setDescription("Token counting flow")
 *        .setFailurePolicy(FailurePolicy.RETRY)
 *        .build();
 * }
 * </pre>
 */
public final class FlowletSpecification {

  private final String name;
  private final String description;
  private final FailurePolicy failurePolicy;

  /**
   * Creates a {@link Builder} for building instance of this class.
   *
   * @return a new builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Private constructor, only called by {@link Builder}.
   */
  private FlowletSpecification(String name, String description, FailurePolicy failurePolicy) {
    this.name = name;
    this.description = description;
    this.failurePolicy = failurePolicy;
  }

  /**
   * @return Name of the flowlet.
   */
  public String getName() {
    return name;
  }

  /**
   * @return Description of the flowlet.
   */
  public String getDescription() {
    return description;
  }

  /**
   * Builder for creating instance of {@link FlowletSpecification}. The builder instance is
   * not reusable, meaning each instance of this class can only be used to create one instance
   * of {@link FlowletSpecification}.
   */
  public static final class Builder {

    private String name;
    private String description;
    private FailurePolicy failurePolicy = FailurePolicy.RETRY;

    /**
     * Private builder to maintain builder contract.
     */
    private Builder() {
    }

    /**
     * Sets the name of a flowlet.
     * @param name of the flowlet.
     * @return An instance of {@link DescriptionSetter}
     */
    public DescriptionSetter setName(String name) {
      Preconditions.checkArgument(name != null, "Name cannot be null.");
      this.name = name;
      return new DescriptionSetter();
    }

    /**
     * Class defining the description setter that is used as part of the builder.
     */
    public final class DescriptionSetter {
      /**
       * Sets the description of the flowlet.
       * @param description to be associated with flowlet.
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
       * @param policy to be associated with flowlet for handling failures of processing
       * @return An instance of {@link AfterDescription}
       */
      public AfterDescription setFailurePolicy(FailurePolicy policy) {
        Preconditions.checkArgument(policy != null, "FailurePolicy cannot be null");
        failurePolicy = policy;
        return this;
      }

      /**
       * Creates an instance of {@link FlowletSpecification}
       * @return An instance of {@link FlowletSpecification}
       */
      public FlowletSpecification build() {
        return new FlowletSpecification(name, description, failurePolicy);
      }
    }
  }
}
