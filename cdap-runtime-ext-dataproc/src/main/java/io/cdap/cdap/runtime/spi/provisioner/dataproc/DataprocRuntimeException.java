/*
 * Copyright © 2020 Cask Data, Inc.
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

/**
 * A {@link RuntimeException} that wraps exceptions from Dataproc operation and provide a {@link #toString()}
 * implementation that doesn't include this exception class name and with the root cause error message.
 */
public class DataprocRuntimeException extends RuntimeException {

  public DataprocRuntimeException(String message) {
    super(message);
  }

  public DataprocRuntimeException(Throwable cause) {
    super(createMessage(cause), cause);
  }

  @Override
  public String toString() {
    return getMessage();
  }

  private static String createMessage(Throwable cause) {
    return String.format("Dataproc operation failure: %s", Throwables.getRootCause(cause).getMessage());
  }
}
