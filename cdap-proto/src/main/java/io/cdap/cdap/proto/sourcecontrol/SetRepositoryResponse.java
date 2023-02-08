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

package io.cdap.cdap.proto.sourcecontrol;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * The HTTP Response class for setting repository configuration.
 */
public class SetRepositoryResponse {
  private final Collection<RepositoryValidationFailure> errors;
  private final String message;

  public SetRepositoryResponse(RepositoryConfigValidationException e) {
    this.errors = Collections.unmodifiableList(new ArrayList<>(e.getFailures()));
    this.message = e.getMessage();
  }

  public SetRepositoryResponse(String message) {
    this.errors = Collections.unmodifiableList(new ArrayList<>());
    this.message = message;
  }

  public String getMessage() {
    return message;
  }

  public Collection<RepositoryValidationFailure> getErrors() {
    return errors;
  }
}
