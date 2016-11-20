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
package co.cask.cdap.logging.serialize;

import co.cask.cdap.api.log.ClassPackagingData;
import co.cask.cdap.api.log.StackTraceElementProxy;

import javax.annotation.Nullable;

/**
 * Implementation of {@link StackTraceElementProxy}
 */
public class StackTraceElementProxyImpl implements StackTraceElementProxy {
  private final StackTraceElement stackTraceElement;
  private final ClassPackagingData classPackagingData;

  public StackTraceElementProxyImpl(StackTraceElement stackTraceElement,
                                    @Nullable ClassPackagingData classPackagingData) {
    this.stackTraceElement = stackTraceElement;
    this.classPackagingData = classPackagingData;
  }

  @Nullable
  @Override
  public ClassPackagingData getClassPackagingData() {
    return classPackagingData;
  }

  @Override
  public StackTraceElement getStackTraceElement() {
    return stackTraceElement;
  }
}
