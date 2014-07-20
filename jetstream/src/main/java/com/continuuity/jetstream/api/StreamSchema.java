/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.jetstream.api;

import com.continuuity.jetstream.internal.DefaultGDATField;
import com.continuuity.jetstream.internal.DefaultStreamSchema;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * Used to define the Schema of a Input Stream to {@link AbstractInputFlowlet}.
 */
public interface StreamSchema {

  List<GDATField> getFields();

  /**
   * Builder for creating instance of {@link StreamSchema}.
   */
  static final class Builder {

    private Set<String> fieldNames = Sets.newHashSet();
    private boolean anyWindowAttributeField = false;
    private List<GDATField> fields = Lists.newArrayList();

    private void fieldCheck(String name) {
      Preconditions.checkArgument(name != null, "Field name cannot be null.");
      Preconditions.checkState(!fieldNames.contains(name), "Field name already used.");
    }

    public Builder addField(String name, GDATFieldType fieldType) {
      fieldCheck(name);
      this.fieldNames.add(name);
      this.fields.add(new DefaultGDATField(name, fieldType));
      return this;
    }

    public Builder addField(String name, GDATFieldType fieldType, GDATSlidingWindowAttribute windowAttribute) {
      fieldCheck(name);
      this.fieldNames.add(name);
      if (windowAttribute != GDATSlidingWindowAttribute.NONE) {
        this.anyWindowAttributeField = true;
      }
      this.fields.add(new DefaultGDATField(name, fieldType, windowAttribute));
      return this;
    }

    public StreamSchema build() {
      //TODO: Add a check to make sure there is at least one increasing or decreasing fields.
      DefaultStreamSchema schema = new DefaultStreamSchema(fields);
      return schema;
    }

  }

}
