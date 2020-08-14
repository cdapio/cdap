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

package io.cdap.cdap.common.lang;

/**
 * Utility class which provides helper methods concerning exception handling.
 */
public class Exceptions {
  public static final String CONDENSE_COMBINER_STRING = System.lineSeparator() + "Caused by: ";

  /**
   * Condenses Throwable messages across an exception chain to a single message string. Ignores null and empty messages.
   * @return The condensed exception string
   */
  public static String condenseThrowableMessage(Throwable t) {
    StringBuilder condensedMessageBuilder = new StringBuilder();
    condensedMessageBuilder.append(t.getMessage());
    Throwable current = t.getCause();
    while (current != null) {
      String currMessage = current.getMessage();
      // Ignore null and empty message strings caused by Java runtime
      if (currMessage != null && currMessage.length() != 0) {
        condensedMessageBuilder.append(CONDENSE_COMBINER_STRING);
        condensedMessageBuilder.append(currMessage);
      }
      current = current.getCause();
    }
    return condensedMessageBuilder.toString();
  }
}
