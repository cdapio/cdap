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

package co.cask.cdap.api.flow.flowlet;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.common.PropertyProvider;
import co.cask.cdap.internal.flowlet.DefaultFlowletSpecification;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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
   * @return An immutable set of {@link co.cask.cdap.api.dataset.Dataset DataSets} name that
   *         used by the {@link Flowlet}.
   */
  Set<String> getDataSets();

  /**
   * @return The {@link Resources} requirements for the flowlet.
   */
  Resources getResources();

  /**
   * Builder for creating instance of {@link FlowletSpecification}. The builder instance is
   * not reusable, meaning each instance of this class can only be used to create one instance
   * of {@link FlowletSpecification}.
   */
  final class Builder {

    private String name;
    private String description;
    private FailurePolicy failurePolicy = FailurePolicy.RETRY;
    private final Set<String> dataSets = new HashSet<>();
    private Map<String, String> arguments;
    private Resources resources = new Resources();

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
        if (name == null) {
          throw new IllegalArgumentException("Name cannot be null.");
        }
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
        if (description == null) {
          throw new IllegalArgumentException("Description cannot be null.");
        }
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
        if (policy == null) {
          throw new IllegalArgumentException("FailurePolicy cannot be null");
        }
        failurePolicy = policy;
        return this;
      }

      /**
       * Adds the names of {@link co.cask.cdap.api.dataset.Dataset DataSets} used by the flowlet.
       *
       * @param dataSet DataSet name.
       * @param moreDataSets More DataSet names.
       * @return An instance of {@link AfterDescription}.
       */
      public AfterDescription useDataSet(String dataSet, String...moreDataSets) {
        dataSets.add(dataSet);
        dataSets.addAll(Arrays.asList(moreDataSets));
        return this;
      }

      /**
       * Adds a map of arguments that would be available to the flowlet through the {@link FlowletContext} at runtime.
       *
       * @param args The map of arguments.
       * @return An instance of {@link AfterDescription}.
       */
      public AfterDescription withArguments(Map<String, String> args) {
        arguments = new HashMap<>(args);
        return this;
      }

      public AfterDescription withResources(Resources resources) {
        if (resources == null) {
          throw new IllegalArgumentException("Resources cannot be null.");
        }
        Builder.this.resources = resources;
        return this;
      }

      /**
       * Creates an instance of {@link FlowletSpecification}.
       * @return An instance of {@link FlowletSpecification}.
       */
      public FlowletSpecification build() {
        return new DefaultFlowletSpecification(name, description, failurePolicy, dataSets, arguments, resources);
      }
    }

    /**
     * Private builder to maintain builder contract.
     */
    private Builder() {
    }
  }
}
