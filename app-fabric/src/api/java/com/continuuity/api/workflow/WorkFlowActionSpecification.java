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
public interface WorkFlowActionSpecification {

  String getName();

  String getDescription();

  final class Builder extends BaseBuilder<WorkFlowActionSpecification> {

    public static NameSetter<DescriptionSetter<Creator<WorkFlowActionSpecification>>> with() {
      Builder builder = new Builder();
      return SimpleNameSetter.create(
        getNameSetter(builder), SimpleDescriptionSetter.create(
        getDescriptionSetter(builder), (Creator<WorkFlowActionSpecification>) builder
      ));
    }

    @Override
    public WorkFlowActionSpecification build() {
      return new WorkFlowActionSpecification() {
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
