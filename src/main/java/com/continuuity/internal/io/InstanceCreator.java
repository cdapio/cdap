package com.continuuity.internal.io;

import com.google.common.reflect.TypeToken;

/**
 *
 */
public interface InstanceCreator<T> {

  T create();
}
