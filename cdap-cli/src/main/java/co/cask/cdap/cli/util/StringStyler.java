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

package co.cask.cdap.cli.util;

/**
 * Styles strings.
 */
public class StringStyler {

  private static final String SET_PLAIN_TEXT = "\033[0;0m";
  private static final String SET_BOLD_TEXT = "\033[0;1m";

  public static String bold(String string) {
    return String.format("%s%s%s", SET_BOLD_TEXT, string, SET_PLAIN_TEXT);
  }

}
