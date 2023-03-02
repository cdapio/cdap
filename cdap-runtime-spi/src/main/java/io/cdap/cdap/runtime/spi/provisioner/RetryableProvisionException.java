/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.provisioner;

import io.cdap.cdap.error.api.ErrorTagProvider;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * An provision exception that indicates a failure that may succeed after a retry.
 */
public class RetryableProvisionException extends Exception implements ErrorTagProvider {

  private final Set<ErrorTag> errorTags = new HashSet<>();

  public RetryableProvisionException(String message) {
    super(message);
    addCommonTags();
  }

  public RetryableProvisionException(Throwable cause) {
    this(cause, null, null);
  }

  public RetryableProvisionException(Throwable cause, ErrorTag... tags) {
    super(cause);
    errorTags.addAll(Arrays.asList(tags));
    addCommonTags();
  }

  public RetryableProvisionException(String message, Throwable cause) {
    super(message, cause);

  }

  private void addCommonTags() {
    errorTags.add(ErrorTag.DEPENDENCY);
    // Provisoning is an internal detail. Even communication issues should probably be considered as SYSTEM
    errorTags.add(ErrorTag.SYSTEM);
  }

  @Override
  public Set<ErrorTag> getErrorTags() {
    return Collections.unmodifiableSet(errorTags);
  }
}
