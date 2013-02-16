/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow;

import com.continuuity.api.flow.QueueSpecification;
import com.continuuity.api.io.UnsupportedTypeException;

import java.lang.reflect.Type;

/**
 *
 */
public interface QueueSpecificationGenerator {
  QueueSpecification generate(String key, Type type) throws UnsupportedTypeException;
}
