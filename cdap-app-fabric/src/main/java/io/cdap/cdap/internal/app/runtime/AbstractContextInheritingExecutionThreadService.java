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

package io.cdap.cdap.internal.app.runtime;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import io.cdap.cdap.proto.security.SecurityContext;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;

/**
 * Should be used by any {@link io.cdap.cdap.app.runtime.ProgramRuntimeService} to inherit
 * {@link io.cdap.cdap.security.spi.authentication.SecurityRequestContext} variables.
 */
public abstract class AbstractContextInheritingExecutionThreadService extends AbstractExecutionThreadService {

  private final InheritableThreadLocal<SecurityContext> securityContextInheritableThreadLocal =
    new InheritableThreadLocal<>();

  @Override
  protected void startUp() throws Exception {
    SecurityRequestContext.set(securityContextInheritableThreadLocal.get());
    startUpInternal();
  }

  public AbstractContextInheritingExecutionThreadService() {
    securityContextInheritableThreadLocal.set(SecurityRequestContext.get());
  }

  protected abstract void startUpInternal() throws Exception;
}
