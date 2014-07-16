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

import com.continuuity.jetstream.internal.DefaultStreamSchema;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.LinkedHashMap;
import java.util.Set;

/**
 * Used to define the Schema of a Input Stream to {@link AbstractInputFlowlet}.
 */
public interface StreamSchema {

  LinkedHashMap<String, PrimitiveType> getFieldNames();

  Set<String> getIncreasingFields();

  Set<String> getDecreasingFields();

  /**
   * Builder for creating instance of {@link StreamSchema}. The builder instance is not reusable, meaning
   * each instance of this class can only be used to create on instance of {@link StreamSchema}.
   */
  static final class Builder {
    private LinkedHashMap<String, PrimitiveType> fieldNames = Maps.newLinkedHashMap();
    private Set<String> increasingFields = Sets.newHashSet();
    private Set<String> decreasingFields = Sets.newHashSet();

    public static FieldSetter with() {
      return new Builder().new FieldSetter();
    }

    public final class FieldSetter {

      private void fieldCheck(String name) {
        Preconditions.checkArgument(name != null, "Field name cannot be null.");
        Preconditions.checkState(!fieldNames.containsKey(name), "Field name already used.");
      }

      public FieldSetter field(String name, PrimitiveType type) {
        fieldCheck(name);
        Builder.this.fieldNames.put(name, type);
        return this;
      }

      public FieldSetter increasingField(String name, PrimitiveType type) {
        fieldCheck(name);
        Builder.this.fieldNames.put(name, type);
        Builder.this.increasingFields.add(name);
        return this;
      }

      public FieldSetter addDecreasingField(String name, PrimitiveType type) {
        fieldCheck(name);
        Builder.this.fieldNames.put(name, type);
        Builder.this.decreasingFields.add(name);
        return this;
      }

      public StreamSchema build() {
        //TODO: Add a check to make sure there is at least one increasing or decreasing fields.
        return new DefaultStreamSchema(fieldNames, increasingFields, decreasingFields);
      }
    }

    private Builder() {
    }

  }

}
