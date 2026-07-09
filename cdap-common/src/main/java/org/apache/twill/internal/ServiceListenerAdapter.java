/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.twill.internal;

import com.google.common.util.concurrent.Service;

/**
 * A shim for {@code org.apache.twill.internal.ServiceListenerAdapter} that extends
 * {@code com.google.common.util.concurrent.Service.Listener} (which is an abstract class
 * in Guava 32+) instead of implementing it (which was an interface in legacy Guava).
 * This resolves {@link IncompatibleClassChangeError} at runtime with Twill.
 */
public abstract class ServiceListenerAdapter extends Service.Listener {

  public ServiceListenerAdapter() {
    super();
  }

  @Override
  public void starting() {
    // No-op
  }

  @Override
  public void running() {
    // No-op
  }

  @Override
  public void stopping(Service.State from) {
    // No-op
  }

  @Override
  public void terminated(Service.State from) {
    // No-op
  }

  @Override
  public void failed(Service.State from, Throwable failure) {
    // No-op
  }
}
