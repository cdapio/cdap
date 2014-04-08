package com.continuuity.internal.app.runtime.service;

import com.continuuity.app.Id;
import com.continuuity.app.program.Type;

/**
 *
 */
public class NotRunningLiveInfo extends LiveInfo {

  public NotRunningLiveInfo(Id.Program programId, Type type) {
    super(programId, type, null);
  }
}
