/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

import io.cdap.cdap.proto.id.SecureKeyId;

/**
 * Thrown when a secure key is not found.
 */
public class SecureKeyNotFoundException extends NotFoundException {
  private final SecureKeyId secureKeyId;

  public SecureKeyNotFoundException(SecureKeyId secureKeyId) {
    super(secureKeyId);
    this.secureKeyId = secureKeyId;
  }

  public SecureKeyNotFoundException(SecureKeyId secureKeyId, Throwable t) {
    super(secureKeyId, secureKeyId.toString(), t);
    this.secureKeyId = secureKeyId;
  }

  public SecureKeyId getId() {
    return secureKeyId;
  }
}
