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

package co.cask.cdap.api.workflow;

import co.cask.cdap.api.common.PropertyProvider;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.internal.workflow.DefaultWorkflowActionSpecification;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Set;

/**
 * Specification for a {@link WorkflowAction}.
 */
public interface WorkflowActionSpecification extends PropertyProvider {

  /**
   * @return Class name of the workflow action.
   */
  String getClassName();

  /**
   * @return Name of the workflow action.
   */
  String getName();

  /**
   * @return Description of the workflow action.
   */
  String getDescription();

  /**
   * @return an immutable set of {@link Dataset} name that are used by the {@link WorkflowAction}
   */
  Set<String> getDatasets();

  /**
   * Builder class for building the {@link WorkflowActionSpecification}.
   */
  static final class Builder {
    private String name;
    private String description;
    private Map<String, String> options = Maps.newHashMap();
    private final ImmutableSet.Builder<String> datasets = ImmutableSet.builder();

    public static NameSetter with() {
      return new Builder().new NameSetter();
    }

    public final class NameSetter {
      /**
       * Sets the name of a action.
       * @param name Name of the action.
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
       * Sets the description of the action.
       * @param description Description to be associated with action.
       * @return An instance of what needs to be done after description {@link AfterDescription}
       */
      public AfterDescription setDescription(String description) {
        Preconditions.checkArgument(description != null, "Description cannot be null.");
        Builder.this.description = description;
        return new AfterDescription();
      }
    }

    /**
     * Class defining the action after defining the description for a action.
     */
    public final class AfterDescription {

      public AfterDescription withOptions(Map<String, String> options) {
        Builder.this.options.putAll(options);
        return this;
      }

      public AfterDescription useDataset(String dataset, String...moreDatasets) {
        Builder.this.datasets.add(dataset).add(moreDatasets);
        return this;
      }

      public WorkflowActionSpecification build() {
        return new DefaultWorkflowActionSpecification(name, description, options, datasets.build());
      }
    }

    /**
     * Private builder to maintain builder contract.
     */
    private Builder() {
    }
  }
}
