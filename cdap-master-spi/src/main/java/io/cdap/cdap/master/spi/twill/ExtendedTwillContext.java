/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.master.spi.twill;

import org.apache.twill.api.TwillContext;

import javax.annotation.Nullable;

/**
 * Extends the {@link TwillContext} to add extra functionalities for CDAP.
 */
public interface ExtendedTwillContext extends TwillContext {

  /**
   * Returns an unique ID of the current container.
   *
   * @return the unique ID, or {@code null} if there is no unique ID in the current execution environment.
   */
  @Nullable
  String getUID();
}
