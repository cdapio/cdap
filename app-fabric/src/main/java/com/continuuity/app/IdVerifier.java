package com.continuuity.app;

import com.continuuity.app.verification.AbstractVerifier;

/**
 * Generic verifier to verify the component ID correctness
 */
public class IdVerifier extends AbstractVerifier<String> {
  @Override
  protected String getName(String id) {
    return id;
  }
}
