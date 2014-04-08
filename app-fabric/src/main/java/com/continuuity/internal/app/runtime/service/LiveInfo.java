package com.continuuity.internal.app.runtime.service;

import com.continuuity.app.Id;
import com.continuuity.app.program.Type;

/**
 * Represents information about running programs. This class can be extended to add information for specific runtime
 * environments.
 */
public abstract class LiveInfo {
  private final String app;
  private final String type;
  private final String id;
  private final String runtime;

  public LiveInfo(Id.Program programId, Type type, String runtime) {
    this.app = programId.getApplicationId();
    this.type = type.prettyName();
    this.id = programId.getId();
    this.runtime = runtime;
  }

  public String getApp() {
    return app;
  }

  public Type getType() {
    return Type.valueOfPrettyName(type);
  }

  public String getId() {
    return id;
  }

  public String getRuntime() {
    return runtime;
  }
}
