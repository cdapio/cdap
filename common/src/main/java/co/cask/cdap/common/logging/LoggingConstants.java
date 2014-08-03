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

package co.cask.cdap.common.logging;

import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/**
 * Common logging constants.
 */
public final class LoggingConstants {
  /**
   * This marker should be used for logs that are emitted within logging context which we don't want to show to user.
   * Since every log message emitted thru slf4j api in place where logging context is available will be injected with
   * info from that context and propagated to the common store of the log messages shown to user, we have to mark it
   * with this marker to avoid log message to be shown to user.
   */
  public static final Marker SYSTEM_MARKER = MarkerFactory.getMarker("-SYSTEM-");
}
