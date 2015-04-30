/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.api.templates;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.schedule.ScheduleSpecification;

import java.lang.reflect.Type;
import javax.annotation.Nullable;

/**
 * Specification of an adapter.
 */
@Beta
public interface AdapterSpecification {

  /**
   * Returns the name of the template that the adapter was created from.
   */
  String getTemplate();

  /**
   * Returns the name of the adapter.
   */
  String getName();

  /**
   * Returns the description of the adapter or {@code null} if there is no description.
   */
  @Nullable
  String getDescription();

  /**
   * Returns the string representation of the configuration used to create the adapter
   * or {@code null} if there is no configuration.
   */
  @Nullable
  String getConfigString();

  /**
   * Returns the configuration object used to create the adapter
   * or {@code null} if there is no configuration.
   *
   * @param configType A {@link Type} representing the configuration object type.
   * @param <T> Type of the configuration object.
   * @return The configuration object
   */
  @Nullable
  <T> T getConfig(Type configType);

  /**
   * Returns the schedule specification of the adapter or {@code null} if there is
   * no schedule associated with the adapter.
   */
  @Nullable
  ScheduleSpecification getScheduleSpecification();
}
