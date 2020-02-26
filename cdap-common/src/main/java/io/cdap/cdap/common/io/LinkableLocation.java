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

package io.cdap.cdap.common.io;

import org.apache.twill.filesystem.Location;

import java.nio.file.Path;

/**
 * A {@link Location} that supports local file link.
 */
public interface LinkableLocation extends Location {

  /**
   * Tries to create a file link to the given target path.
   *
   * @param target the target path to link to
   * @return {@code true} if the link was created successfully; otherwise return {@code false}
   */
  boolean tryLink(Path target);
}
