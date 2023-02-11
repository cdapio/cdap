/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.sourcecontrol;

/**
 * Exception thrown when config file path in git is resolving to an invalid path
 * i.e. path outside repository root
 */
public class InvalidPathInSourceControl extends Exception {
  public InvalidPathInSourceControl(String message, Exception cause) {
    super(message, cause);
  }

  public InvalidPathInSourceControl(String message) {
    super(message);
  }
}
