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
