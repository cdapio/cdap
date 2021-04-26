/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.internal.app.runtime.plugin;

import com.google.gson.FieldNamingStrategy;
import io.cdap.cdap.api.annotation.Name;

import java.lang.reflect.Field;

/**
 * Let gson use the name of the annotation when deserialize the object
 */
public class PluginFieldNamingStrategy implements FieldNamingStrategy {

  @Override
  public String translateName(Field field) {
    Name nameAnnotation = field.getAnnotation(Name.class);
    return nameAnnotation == null ? field.getName() : nameAnnotation.value();
  }
}
