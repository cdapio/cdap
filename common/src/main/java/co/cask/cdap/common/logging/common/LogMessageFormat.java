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

package co.cask.cdap.common.logging.common;

import co.cask.cdap.common.logging.LoggingContext;

/**
 * Defines format for log message text. We need it to be able to later parse log messages (incl.
 * parsing multi-line messages, parsing tags etc.)
 */
public interface LogMessageFormat {
  String format(String message, String[] traceLines, LoggingContext context, String[] userTags);
}
