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

import com.continuuity.jetstream.api.GDATField;
import com.continuuity.jetstream.api.GDATFieldType;
import com.continuuity.jetstream.api.GDATSlidingWindowAttribute;

/**
 *
 */
public class DefaultGDATField implements GDATField {

  private String name;
  private GDATFieldType fieldType;
  private GDATSlidingWindowAttribute windowType;

  public DefaultGDATField(String name, GDATFieldType fieldType, GDATSlidingWindowAttribute windowType) {
    this.name = name;
    this.fieldType = fieldType;
    this.windowType = windowType;
  }

  public DefaultGDATField(String name, GDATFieldType fieldType) {
    this.name = name;
    this.fieldType = fieldType;
    this.windowType = GDATSlidingWindowAttribute.NONE;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public GDATFieldType getType() {
    return fieldType;
  }

  @Override
  public GDATSlidingWindowAttribute getSlidingWindowType() {
    return windowType;
  }
}
