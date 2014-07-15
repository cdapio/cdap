/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.api.workflow;

import com.continuuity.api.builder.Creator;
import com.continuuity.api.builder.DescriptionSetter;
import com.continuuity.api.builder.NameSetter;
import com.continuuity.api.builder.OptionsSetter;
import com.continuuity.api.common.PropertyProvider;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.internal.builder.BaseBuilder;
import com.continuuity.internal.builder.SimpleDescriptionSetter;
import com.continuuity.internal.builder.SimpleNameSetter;
import com.continuuity.internal.workflow.DefaultWorkflowActionSpecification;
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
