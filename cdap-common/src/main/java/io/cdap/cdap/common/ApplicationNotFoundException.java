/*
 * Copyright © 2014-2022 Cask Data, Inc.
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

package io.cdap.cdap.common;

import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;

/**
 * Thrown when an application is not found.
 */
public class ApplicationNotFoundException extends NotFoundException {

  private final ApplicationReference applicationReference;

  public ApplicationNotFoundException(ApplicationId appId) {
    super(appId);
    this.applicationReference = appId.getAppReference();
  }

  public ApplicationNotFoundException(ApplicationReference appReference) {
    super(appReference);
    this.applicationReference = appReference;
  }

  public ApplicationReference getApplicationReference() {
    return applicationReference;
  }
}
