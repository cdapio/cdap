/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.procedure;

import com.continuuity.internal.api.procedure.DefaultProcedureSpecification;
import com.google.common.base.Preconditions;

/**
 * This class defines an specification for a {@link Procedure}.
 * An {@link Procedure} runtime attributes are always defined using this class.
 * Instance of this class should be created through the {@link Builder} class
 * by invoking the {@link Builder#with()} method.
 */
public interface ProcedureSpecification {

  /**
   * @return Class name of the {@link Procedure} class.
   */
  String getClassName();

  /**
   * @return Name of the {@link Procedure}
   */
  String getName();

  /**
   * @return Description to be associated with {@link Procedure}
   */
  String getDescription();

  /**
   * Builder for building {@link ProcedureSpecification}
   */
  static final class Builder {
    private String name;
    private String description;

    public static Builder with() {
      return new Builder();
    }

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
        return new DefaultProcedureSpecification(name, description);
      }
    }
    private Builder() {}
  }
}
