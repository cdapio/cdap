/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import org.apache.twill.api.Command;

/**
 * The class carries {@link Command} that are used by the flow system.
 */
public final class ProgramCommands {

  public static final Command SUSPEND = Command.Builder.of("suspend").build();
  public static final Command RESUME = Command.Builder.of("resume").build();

  public static Command createSetInstances(int instances) {
    return Command.Builder.of(ProgramOptionConstants.INSTANCES).addOption("count", Integer.toString(instances)).build();
  }

  private ProgramCommands() {
  }
}
