package com.continuuity.api.flow.flowlet;

import com.google.common.base.Preconditions;

/**
 *
 */
public final class FlowletSpecification {

  private final String name;
  private final String description;
  private final FailurePolicy failurePolicy;

  public static Builder builder() {
    return new Builder();
  }

  private FlowletSpecification(String name, String description, FailurePolicy failurePolicy) {
    this.name = name;
    this.description = description;
    this.failurePolicy = failurePolicy;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public static final class Builder {

    private String name;
    private String description;
    private FailurePolicy failurePolicy = FailurePolicy.RETRY;

    private Builder() {
    }

    public DescriptionSetter setName(String name) {
      Preconditions.checkArgument(name != null, "Name cannot be null.");
      this.name = name;
      return new DescriptionSetter();
    }

    public final class DescriptionSetter {
      public AfterDescription setDescription(String description) {
        Preconditions.checkArgument(description != null, "Description cannot be null.");
        Builder.this.description = description;
        return new AfterDescription();
      }
    }

    public final class AfterDescription {

      public AfterDescription setFailurePolicy(FailurePolicy policy) {
        Preconditions.checkArgument(policy != null, "FailurePolicy cannot be null");
        failurePolicy = policy;
        return this;
      }

      public FlowletSpecification build() {
        return new FlowletSpecification(name, description, failurePolicy);
      }
    }
  }
}
