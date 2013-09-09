package com.continuuity.api.workflow;

import com.continuuity.api.builder.Creator;
import com.continuuity.api.builder.DescriptionSetter;
import com.continuuity.api.builder.NameSetter;
import com.continuuity.internal.builder.BaseBuilder;
import com.continuuity.internal.builder.SimpleDescriptionSetter;
import com.continuuity.internal.builder.SimpleNameSetter;

/**
 *
 */
public interface WorkflowActionSpecification {

  String getName();

  String getDescription();

  final class Builder extends BaseBuilder<WorkflowActionSpecification> {

    public static NameSetter<DescriptionSetter<Creator<WorkflowActionSpecification>>> with() {
      Builder builder = new Builder();
      return SimpleNameSetter.create(
        getNameSetter(builder), SimpleDescriptionSetter.create(
        getDescriptionSetter(builder), (Creator<WorkflowActionSpecification>) builder
      ));
    }

    @Override
    public WorkflowActionSpecification build() {
      return new WorkflowActionSpecification() {
        @Override
        public String getName() {
          return name;
        }

        @Override
        public String getDescription() {
          return description;
        }
      };
    }

    private Builder() {
    }
  }
}
