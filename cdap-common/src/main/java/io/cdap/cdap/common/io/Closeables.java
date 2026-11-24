/*
 * Copyright © 2026 Cask Data, Inc.
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

import java.io.Closeable;
import java.io.IOException;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps the Guava 13 {@code Closeables.closeQuietly(Closeable)} behavior, which was removed from
 * later Guava releases: a {@code null} argument is ignored and an {@link IOException} from
 * {@link Closeable#close()} is logged and swallowed. Other exceptions still propagate.
 */
public final class Closeables {

  private static final Logger LOG = LoggerFactory.getLogger(Closeables.class);

  private Closeables() {
  }

  /**
   * Closes the given {@link Closeable}, logging and swallowing any {@link IOException}.
   */
  public static void closeQuietly(@Nullable Closeable closeable) {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
    } catch (IOException e) {
      LOG.warn("IOException thrown while closing Closeable.", e);
    }
  }
}
