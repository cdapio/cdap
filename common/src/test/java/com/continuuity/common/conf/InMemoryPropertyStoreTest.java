/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.conf;

import com.continuuity.common.io.Codec;

/**
 *
 */
public class InMemoryPropertyStoreTest extends PropertyStoreTestBase {

  @Override
  protected <T> PropertyStore<T> createPropertyStore(Codec<T> codec) {
    return new InMemoryPropertyStore<T>();
  }
}
