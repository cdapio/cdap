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

package co.cask.cdap.security.zookeeper;

/**
 * Simple {@link ResourceListener} implementation with no-op implementations.  Implementers interested only
 * in handling specific events can subclass this class and override the handler methods that they care about.
 * @param <T> The resource type being managed by {@code SharedResourceCache}.
 */
public class BaseResourceListener<T> implements ResourceListener<T> {
  @Override
  public void onUpdate() {
    // no-op
  }

  @Override
  public void onResourceUpdate(String name, T instance) {
    // no-op
  }

  @Override
  public void onResourceDelete(String name) {
    // no-op
  }

  @Override
  public void onError(String name, Throwable throwable) {
    // no-op
  }
}
