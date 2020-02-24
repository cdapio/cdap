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
import java.util.Optional;
import java.util.function.Function;

/**
 * Provides local {@link Path} for caching {@link Location}.
 */
public interface CachingPathProvider extends Function<Location, Optional<Path>> {

  /**
   * Returns an optional local path for caching for the given location.
   *
   * @param location the {@link Location} for getting the local cache path.
   * @return a local file {@link Path} in the {@link Optional} or an empty {@link Optional} if no caching is needed.
   */
  @Override
  Optional<Path> apply(Location location);
}
