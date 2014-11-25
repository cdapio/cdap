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

package co.cask.cdap.api.procedure;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.common.PropertyProvider;
import co.cask.cdap.internal.procedure.DefaultProcedureSpecification;
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
 *
 * @deprecated As of version 2.6.0, replaced by {@link co.cask.cdap.api.service.ServiceSpecification}
 */
@Deprecated
public interface ProcedureSpecification extends ProgramSpecification, PropertyProvider {

  /**
   * @return An immutable set of {@link co.cask.cdap.api.dataset.Dataset DataSets} that
   *         are used by the {@link Procedure}.
   */
  Set<String> getDataSets();

  /**
   * @return The {@link Resources} requirements for the procedure, containing how many resources to use.
   */
  Resources getResources();

  /**
   * @return number of instances.
   */
  int getInstances();

  /**
   * Builder for building {@link ProcedureSpecification}.
   *
   * @deprecated As of version 2.6.0, with no direct replacement, see {@link co.cask.cdap.api.service.Service}
   */
  @Deprecated
  static final class Builder {
    private String name;
    private String description;
    private Map<String, String> arguments;
    private Resources resources = new Resources();
    private final ImmutableSet.Builder<String> dataSets = ImmutableSet.builder();

    public static NameSetter with() {
      return new Builder().new NameSetter();
    }

    /**
     * Class for setting name.
     * @deprecated As of version 2.6.0, with no direct replacement, see {@link co.cask.cdap.api.service.Service}
     */
    @Deprecated
    public final class NameSetter {

      /**
       * Sets the name of the {@link Procedure}.
       * @param name of the procedure.
       * @return instance of this {@link Builder}
       * @deprecated As of version 2.6.0, with no direct replacement, see {@link co.cask.cdap.api.service.Service}
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
     * @deprecated As of version 2.6.0, with no direct replacement, see {@link co.cask.cdap.api.service.Service}
     */
    @Deprecated
    public final class DescriptionSetter {

      /**
       * Sets the description for this {@link Procedure}.
       * @param description of the {@link Procedure}
       * @return An instance of {@link AfterDescription}
       * @deprecated As of version 2.6.0, with no direct replacement, see {@link co.cask.cdap.api.service.Service}
       */
      @Deprecated
      public AfterDescription setDescription(String description) {
        Preconditions.checkArgument(description != null, "Description cannot be null.");
        Builder.this.description = description;
        return new AfterDescription();
      }
    }

    /**
     * Part of builder for defining next steps after providing description.
     * @deprecated As of version 2.6.0, with no direct replacement, see {@link co.cask.cdap.api.service.Service}
     */
    @Deprecated
    public final class AfterDescription {

      /**
       * Adds the names of {@link co.cask.cdap.api.dataset.Dataset DataSets} used by the procedure.
       *
       * @param dataSet DataSet name.
       * @param moreDataSets More DataSet names.
       * @return An instance of {@link AfterDescription}.
       * @deprecated As of version 2.6.0, with no direct replacement, see {@link co.cask.cdap.api.service.Service}
       */
      @Deprecated
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
       * @deprecated As of version 2.6.0, with no direct replacement, see {@link co.cask.cdap.api.service.Service}
       */
      @Deprecated
      public AfterDescription withArguments(Map<String, String> args) {
        arguments = ImmutableMap.copyOf(args);
        return this;
      }

      /**
       * 
       * @deprecated As of version 2.6.0, with no direct replacement, see {@link co.cask.cdap.api.service.Service}
       */
      @Deprecated
      public AfterDescription withResources(Resources resources) {
        Preconditions.checkArgument(resources != null, "Resources cannot be null.");
        Builder.this.resources = resources;
        return this;
      }

      /**
       * @return build a {@link ProcedureSpecification}
       * @deprecated As of version 2.6.0, with no direct replacement, see {@link co.cask.cdap.api.service.Service}
       */
      @Deprecated
      public ProcedureSpecification build() {
        return new DefaultProcedureSpecification(name, description, dataSets.build(), arguments, resources);
      }
    }

    private Builder() {}
  }
}
