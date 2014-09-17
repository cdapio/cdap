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

package co.cask.cdap.internal.builder;

import co.cask.cdap.api.builder.Creator;

/**
 * @param <T>
 */
public abstract class BaseBuilder<T> implements Creator<T> {

  protected String name;
  protected String description;

  protected static Setter<String> getNameSetter(final BaseBuilder builder) {
    return new Setter<String>() {
      @Override
      public void set(String obj) {
        builder.name = obj;
      }
    };
  }

  protected static Setter<String> getDescriptionSetter(final BaseBuilder builder) {
    return new Setter<String>() {
      @Override
      public void set(String obj) {
        builder.description = obj;
      }
    };
  }
}
