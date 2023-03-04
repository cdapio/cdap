/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.k8s.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Contains Kubernetes-specific helper methods.
 */
public class KubeUtil {

  private static final Set<String> PATH_SEGMENT_ILLEGAL_NAMES
      = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(".", "..")));

  private static final Set<Character> PATH_SEGMENT_NAME_ILLEGAL_CHARACTERS
      = Collections.unmodifiableSet(new HashSet<>(Arrays.asList('/', '%')));

  private static final Pattern RFC1123_VALIDATION_REGEX = Pattern.compile(
      "[a-z0-9]([-a-z0-9]*[a-z0-9])?");

  /**
   * Validates that a given name can be properly encoded as a path segment. For details, see
   * https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#path-segment-names.
   *
   * @param name The name to validate
   * @throws IllegalArgumentException If the name is invalid
   */
  public static void validatePathSegmentName(String name) throws IllegalArgumentException {
    if (PATH_SEGMENT_ILLEGAL_NAMES.contains(name)) {
      throw new IllegalArgumentException(
          String.format("Illegal path segment name '%s': name cannot be '.' or '..'",
              name));
    }

    for (int i = 0; i < name.length(); i++) {
      if (PATH_SEGMENT_NAME_ILLEGAL_CHARACTERS.contains(name.charAt(i))) {
        throw new IllegalArgumentException(
            String.format("Illegal path segment name '%s': name contains illegal "
                + "characters '/' or '%%' at position %d", name, i));
      }
    }
  }

  /**
   * Validates that a given name is a valid RFC-1123 DNS label name. A valid RFC-1123 DNS label: - *
   * contains at most 63 characters - contains only lowercase alphanumeric characters or '-' - *
   * starts with an alphanumeric character - ends with an alphanumeric character
   *
   * For details, see https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-label-names
   * and https://datatracker.ietf.org/doc/html/rfc1123#section-2.
   *
   * @param name The name to validate
   * @throws IllegalArgumentException If the name is not a valid RFC-1123 DNS Label
   */
  public static void validateRFC1123LabelName(String name) throws IllegalArgumentException {
    if (name.length() > 63) {
      throw new IllegalArgumentException(
          String.format("Illegal RFC-1123 name '%s': Must be no longer than 63 "
              + "characters", name));
    }
    if (!RFC1123_VALIDATION_REGEX.matcher(name).matches()) {
      throw new IllegalArgumentException(
          String.format("Illegal RFC-1123 name '%s': must start and end with an "
              + "alphanumeric character, and may only contain alphanumeric "
              + "characters and '-'.", name));
    }
  }
}
