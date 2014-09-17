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

package co.cask.cdap.api.spark;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.common.PropertyProvider;
import co.cask.cdap.internal.spark.DefaultSparkSpecification;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * This class provides the specification for a Spark job. Create instances of this class via the {@link Builder}
 * class by invoking the {@link Builder#with()} method.
 * <p>
 * Example:
 * <pre>
 * {@code
 * SparkSpecification spec =
 *     new SparkSpecification.Builder.with()
 *       .setName("AggregateMetricsByTag")
 *       .setDescription("Aggregates metrics values by tag")
 *       .setMainClass("com.example.SomeJob")
 *       .build();
 * }
 * </pre>
 * </p>
 */
public interface SparkSpecification extends ProgramSpecification, PropertyProvider {

  /**
   * @return the Spark job main class name
   */
  String getMainClassName();

  /**
   * Builder for building {@link SparkSpecification}.
   */
  static final class Builder {
    private String name;
    private String description;
    private String mainClassName;
    private Map<String, String> arguments;

    /**
     * Start defining {@link SparkSpecification}.
     *
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
       * Sets the name of the {@link Spark} job.
       *
       * @param name Name of the Spark job.
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
       * Sets the description for this {@link Spark} job.
       *
       * @param description Description of the {@link Spark} job.
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
       * Sets the Spark job main class name in specification. The main method of this class will be called to run the
       * Spark job
       *
       * @param mclassName the fully qualified name of class containing the main method
       * @return An instance of {@link AfterDescription}.
       */
      public AfterDescription setMainClassName(String mclassName) {
        mainClassName = mclassName;
        return this;
      }

      /**
       * Adds a map of arguments that would be available to the spark job
       * through the {@link SparkContext} at runtime.
       *
       * @param args The map of arguments.
       * @return An instance of {@link AfterDescription}.
       */
      public AfterDescription withArguments(Map<String, String> args) {
        arguments = ImmutableMap.copyOf(args);
        return this;
      }

      /**
       * @return build a {@link SparkSpecification}
       */
      public SparkSpecification build() {
        return new DefaultSparkSpecification(name, description, mainClassName, arguments);
      }
    }

    private Builder() {
    }
  }
}
