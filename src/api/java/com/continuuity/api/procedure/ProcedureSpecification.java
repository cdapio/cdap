package com.continuuity.api.procedure;

import com.google.common.base.Preconditions;

/**
 * This class defines an specification for a {@link Procedure}.
 * An {@link Procedure} runtime attributes are always defined using this class.
 */
public class ProcedureSpecification {
  private final String name;
  private final String description;

  private ProcedureSpecification(String name, String description) {
    this.name = name;
    this.description = description;
  }

  /**
   * @return Name of the {@link Procedure}
   */
  public String getName() {
    return name;
  }

  /**
   * @return Description to be associated with {@link Procedure}
   */
  public String getDescription() {
    return description;
  }

  /**
   * @return A instance of builder for {@link ProcedureSpecification}
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for building {@link ProcedureSpecification}
   */
  public static final class Builder {
    private String name;
    private String description;

    /**
     * Sets the name of the {@link Procedure}
     * @param name of the procedure.
     * @return instance of this {@link Builder}
     */
    public DescriptionSetter setName(String name) {
      Preconditions.checkArgument(name != null, "Name cannot be null.");
      this.name = name;
      return new DescriptionSetter();
    }

    /**
     * Description setter for builder that guides you through process of building
     * the specification.
     */
    public final class DescriptionSetter {
      /**
       * Sets the description for this {@link Procedure}
       * @param description of the {@link Procedure}
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
       * @return build a {@link ProcedureSpecification}
       */
      public ProcedureSpecification build() {
        return new ProcedureSpecification(name, description);
      }
    }
    private Builder() {}
  }
}
