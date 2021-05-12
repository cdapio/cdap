/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.worker.api;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Represents a context for a {@link RunnableTask}.
 * This context is used for writing back the result of {@link RunnableTask} execution.
 */
public class RunnableTaskContext {
  private final ByteArrayOutputStream outputStream;
  private final String param;

  public RunnableTaskContext(String param) {
    this.param = param;
    this.outputStream = new ByteArrayOutputStream();
  }

  public void writeResult(byte[] data) throws IOException {
    outputStream.write(data);
  }

  public byte[] getResult() {
    return outputStream.toByteArray();
  }

  public String getParam() {
    return param;
  }
}
