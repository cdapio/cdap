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

package io.cdap.cdap.runtime.spi.provisioner.dataproc;

import com.google.common.base.Throwables;
import io.cdap.cdap.runtime.spi.provisioner.RetryableProvisionException;
import javax.annotation.Nullable;

/**
 * An exception thrown while performing a Dataproc operation that may succeed after a retry.
 */
public class DataprocRetryableException extends RetryableProvisionException {

  public DataprocRetryableException(@Nullable String operationId, Throwable cause) {
    super(createMessage(operationId, cause), cause);
  }

  private static String createMessage(@Nullable String operationId, Throwable cause) {
    if (operationId != null) {
      return String.format("Dataproc operation %s failure: %s",
          operationId, Throwables.getRootCause(cause).getMessage());
    } else {
      return String.format("Dataproc operation failure: %s",
          Throwables.getRootCause(cause).getMessage());
    }
  }
}
