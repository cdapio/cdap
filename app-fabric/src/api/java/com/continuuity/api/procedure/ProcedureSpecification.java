/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.procedure;

import com.continuuity.internal.procedure.DefaultProcedureSpecification;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

/**
 * This class defines a specification for a {@link Procedure}.
 * A {@link Procedure} runtime attributes are always defined using this class.
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
   * @return An immutable set of {@link com.continuuity.api.data.DataSet DataSets} that
   *         are used by the {@link Procedure}.
   */
  Set<String> getDataSets();

  /**
   * @return An immutable map of arguments that was passed in when constructing the {@link ProcedureSpecification}.
   */
  Map<String, String> getArguments();

  /**
   * Builder for building {@link ProcedureSpecification}.
   */
  static final class Builder {
    private String name;
    private String description;
    private Map<String, String> arguments;
    private final ImmutableSet.Builder<String> dataSets = ImmutableSet.builder();

    public static NameSetter with() {
      return new Builder().new NameSetter();
    }

    /**
     * Class for setting name.
     */
    public final class NameSetter {

      /**
       * Sets the name of the {@link Procedure}.
       * @param name of the procedure.
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
       * Sets the description for this {@link Procedure}.
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
       * Adds the names of {@link com.continuuity.api.data.DataSet DataSets} used by the procedure.
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
       * Adds a map of arguments that would be available to the procedure
       * through the {@link ProcedureContext} at runtime.
       *
       * @param args The map of arguments.
       * @return An instance of {@link AfterDescription}.
       */
      public AfterDescription withArguments(Map<String, String> args) {
        arguments = ImmutableMap.copyOf(args);
        return this;
      }

      /**
       * @return build a {@link ProcedureSpecification}
       */
      public ProcedureSpecification build() {
        return new DefaultProcedureSpecification(name, description, dataSets.build(), arguments);
      }
    }

    private Builder() {}
  }
}
