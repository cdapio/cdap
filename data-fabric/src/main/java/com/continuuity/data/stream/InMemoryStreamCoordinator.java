/*
 * Copyright 2012-2014 Continuuity, Inc.
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
package com.continuuity.data.stream;

import com.continuuity.common.conf.InMemoryPropertyStore;
import com.continuuity.common.conf.PropertyStore;
import com.continuuity.common.io.Codec;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * In memory implementation for {@link StreamCoordinator}.
 */
@Singleton
public final class InMemoryStreamCoordinator extends AbstractStreamCoordinator {

  @Inject
  protected InMemoryStreamCoordinator(StreamAdmin streamAdmin) {
    super(streamAdmin);
  }

  @Override
  protected <T> PropertyStore<T> createPropertyStore(Codec<T> codec) {
    return new InMemoryPropertyStore<T>();
  }
}
