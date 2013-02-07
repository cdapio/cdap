/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow.flowlet;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 *
 */
public interface StreamEvent {

  Map<String, String> getHeaders();

  ByteBuffer getBody();
}
