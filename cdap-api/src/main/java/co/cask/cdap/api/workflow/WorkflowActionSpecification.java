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

import co.cask.cdap.api.builder.Creator;
import co.cask.cdap.api.builder.DescriptionSetter;
import co.cask.cdap.api.builder.NameSetter;
import co.cask.cdap.api.builder.OptionsSetter;
import co.cask.cdap.api.common.PropertyProvider;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.internal.builder.BaseBuilder;
import co.cask.cdap.internal.builder.SimpleDescriptionSetter;
import co.cask.cdap.internal.builder.SimpleNameSetter;
import co.cask.cdap.internal.workflow.DefaultWorkflowActionSpecification;
import com.google.common.collect.Maps;

import java.util.Map;

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
   * Builder interface for the last stage of building a {@link WorkflowActionSpecification}.
   */
  interface SpecificationCreator extends Creator<WorkflowActionSpecification>,
                                         OptionsSetter<Creator<WorkflowActionSpecification>> { }

  /**
   * Builder class for building the {@link WorkflowActionSpecification}.
   */
  final class Builder extends BaseBuilder<WorkflowActionSpecification> implements SpecificationCreator {

    private final Map<String, MapReduceSpecification> mapReduces = Maps.newHashMap();
    private final Map<String, String> options = Maps.newHashMap();

    public static NameSetter<DescriptionSetter<SpecificationCreator>> with() {
      Builder builder = new Builder();
      return SimpleNameSetter.create(
        getNameSetter(builder), SimpleDescriptionSetter.create(
        getDescriptionSetter(builder), (SpecificationCreator) builder
      ));
    }

    @Override
    public Creator<WorkflowActionSpecification> withOptions(Map<String, String> options) {
      this.options.putAll(options);
      return this;
    }

    @Override
    public WorkflowActionSpecification build() {
      return new DefaultWorkflowActionSpecification(name, description, options);
    }

    private Builder() {
    }
  }
}
