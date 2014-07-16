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

package com.continuuity.jetstream.internal;

import com.continuuity.jetstream.api.StreamSchema;
import com.continuuity.jetstream.api.PrimitiveType;

import java.util.LinkedHashMap;
import java.util.Set;

/**
 * Default StreamSchema.
 */
public class DefaultStreamSchema implements StreamSchema {

  private LinkedHashMap<String, PrimitiveType> fieldNames;
  private Set<String> increasingFields;
  private Set<String> decreasingFields;

  public DefaultStreamSchema(LinkedHashMap<String, PrimitiveType> fieldNames, Set<String> increasingFields,
                             Set<String> decreasingFields) {
    this.fieldNames = fieldNames;
    this.increasingFields = increasingFields;
    this.decreasingFields = decreasingFields;
  }

  @Override
  public LinkedHashMap<String, PrimitiveType> getFieldNames() {
    return fieldNames;
  }

  @Override
  public Set<String> getIncreasingFields() {
    return increasingFields;
  }

  @Override
  public Set<String> getDecreasingFields() {
    return decreasingFields;
  }

}
