package com.continuuity.api.workflow;

import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.api.builder.Creator;
import com.continuuity.api.builder.DescriptionSetter;
import com.continuuity.api.builder.NameSetter;
import com.continuuity.api.builder.OptionsSetter;
import com.continuuity.internal.builder.BaseBuilder;
import com.continuuity.internal.builder.SimpleDescriptionSetter;
import com.continuuity.internal.builder.SimpleNameSetter;
import com.continuuity.internal.workflow.DefaultWorkflowActionSpecification;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Specification for {@link WorkflowAction}.
 */
public interface WorkflowActionSpecification {

  /**
   * @return Class name for the action.
   */
  String getClassName();

  /**
   * @return Name for the action.
   */
  String getName();

  /**
   * @return Descriptive message for the action.
   */
  String getDescription();

  /**
   * @return An immutable map of arguments that was passed in when constructing the {@link WorkflowActionSpecification}.
   */
  Map<String, String> getOptions();

  /**
   * Builder interface for the last stage of building {@link WorkflowActionSpecification}.
   */
  interface SpecificationCreator extends Creator<WorkflowActionSpecification>,
                                         OptionsSetter<Creator<WorkflowActionSpecification>> { }

  /**
   * Builder class for building {@link WorkflowActionSpecification}.
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
