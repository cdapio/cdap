/*
 * Copyright © 2016 Cask Data, Inc.
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
package io.cdap.cdap.internal.app.preview;

import io.cdap.cdap.api.preview.DataTracer;

/**
 * Implementation of the {@link DataTracer} which logs nothing.
 */
class NoopDataTracer implements DataTracer {

  private final String tracerName;

  NoopDataTracer(String tracerName) {
    this.tracerName = tracerName;
  }

  @Override
  public void info(String propertyName, Object propertyValue) {
    // no-op
  }

  @Override
  public String getName() {
    return tracerName;
  }

  @Override
  public boolean isEnabled() {
    return false;
  }

  @Override
  public int getMaximumTracedRecords() {
    return 0;
  }
}
