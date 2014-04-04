package com.continuuity.internal.app.runtime.service;

import com.continuuity.app.Id;
import com.continuuity.app.program.Type;

/**
 * A live info for in-memory runtime envirnment.
 */
public class InMemoryLiveInfo extends LiveInfo {

  public InMemoryLiveInfo(Id.Program programId, Type type) {
    super(programId, type, "in-memory");
  }
}
