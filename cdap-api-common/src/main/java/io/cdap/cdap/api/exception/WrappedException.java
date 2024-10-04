/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.api.exception;

import io.cdap.cdap.error.api.ErrorTagProvider;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Sets the stage name in exception.
 */
public class WrappedException extends RuntimeException implements ErrorTagProvider {

  private final String stageName;
  private final Set<ErrorTag> errorTags = new HashSet<>();

  public WrappedException(Throwable cause, String stageName) {
    super(cause);
    this.stageName = stageName;
    this.errorTags.add(ErrorTag.PLUGIN);
  }

  public WrappedException(String message, String stageName) {
    super(message);
    this.stageName = stageName;
    this.errorTags.add(ErrorTag.PLUGIN);
  }

  public WrappedException(Throwable cause, String message, String stageName) {
    super(message, cause);
    this.stageName = stageName;
    this.errorTags.add(ErrorTag.PLUGIN);
  }

  public String getStageName() {
    return stageName;
  }

  @Override
  public Set<ErrorTag> getErrorTags() {
    return Collections.unmodifiableSet(errorTags);
  }
}
