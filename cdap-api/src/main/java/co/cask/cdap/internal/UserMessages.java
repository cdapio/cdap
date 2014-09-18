/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal;

import java.util.ResourceBundle;

/**
 * User Messages helper class.
 */
public final class UserMessages {

  private static final String BUNDLE_NAME = "UserMessages";

  public static String getMessage(final String key) {

    try {
      return getBundle().getString(key);

    } catch (Exception e) {
      return "Unknown Error. Please check the CDAP Instance log.";
    }

  }

  /**
   *
   * @return Resource bundle.
   */
  public static ResourceBundle getBundle() {

    return ResourceBundle.getBundle(BUNDLE_NAME);

  }

}
