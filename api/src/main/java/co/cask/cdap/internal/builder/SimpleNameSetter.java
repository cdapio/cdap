/*
 * Copyright 2014 Cask, Inc.
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

import co.cask.cdap.api.builder.NameSetter;

/**
 * @param <T>
 */
public final class SimpleNameSetter<T> implements NameSetter<T> {

  private final Setter<String> setter;
  private final T next;

  public static <T> NameSetter<T> create(Setter<String> setter, T next) {
    return new SimpleNameSetter<T>(setter, next);
  }

  private SimpleNameSetter(Setter<String> setter, T next) {
    this.setter = setter;
    this.next = next;
  }

  @Override
  public T setName(String name) {
    setter.set(name);
    return next;
  }
}
