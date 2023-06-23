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

package io.cdap.cdap.internal.credential;

import io.cdap.cdap.common.BadRequestException;
import java.util.regex.Pattern;

/**
 * Utility methods for credential managers.
 */
public class CredentialManagerUtils {

  private static final Pattern RESOURCE_NAME_REGEX = Pattern.compile("[a-z][a-z0-9-]*");

  static void validateResourceName(String name) throws BadRequestException {
    if (!RESOURCE_NAME_REGEX.matcher(name).matches()) {
      throw new BadRequestException(String.format("Invalid resource name. Resource name must match "
          + "the regex '%s'", RESOURCE_NAME_REGEX.pattern()));
    }
  }
}
