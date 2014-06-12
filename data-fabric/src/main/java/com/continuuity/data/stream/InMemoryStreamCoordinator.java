/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
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
