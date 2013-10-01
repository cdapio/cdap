/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.internal.app.runtime.webapp.WebappProgramRunner;

/**
 * Weave runnable wrapper for webapp.
 */
final class WebappWeaveRunnable extends AbstractProgramWeaveRunnable<WebappProgramRunner> {

  WebappWeaveRunnable(String name, String hConfName, String cConfName) {
    super(name, hConfName, cConfName);
  }

  @Override
  protected Class<WebappProgramRunner> getProgramClass() {
    return WebappProgramRunner.class;
  }
}
