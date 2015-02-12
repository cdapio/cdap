/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
package co.cask.cdap.data.stream;

import co.cask.cdap.common.conf.InMemoryPropertyStore;
import co.cask.cdap.common.conf.PropertyStore;
import co.cask.cdap.common.io.Codec;
import co.cask.cdap.proto.Id;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * In memory implementation for {@link StreamCoordinatorClient}.
 */
@Singleton
public final class InMemoryStreamCoordinatorClient extends AbstractStreamCoordinatorClient {

  // Global lock for all streams
  private final Lock lock;

  @Inject
  public InMemoryStreamCoordinatorClient() {
    super();
    this.lock = new ReentrantLock();
  }

  @Override
  protected void doStartUp() throws Exception {
    // No-op
  }

  @Override
  protected void doShutDown() throws Exception {
    // No-op
  }

  @Override
  protected <T> PropertyStore<T> createPropertyStore(Codec<T> codec) {
    return new InMemoryPropertyStore<T>();
  }

  @Override
  protected Lock getLock(Id.Stream streamId) {
    return lock;
  }

  @Override
  protected void streamCreated(Id.Stream streamId) {
    // Nothing to do
  }
}
