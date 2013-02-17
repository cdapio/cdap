package com.continuuity.app.runtime;

import com.continuuity.app.program.Id;
import com.continuuity.app.program.Program;
import com.continuuity.filesystem.Location;

import java.util.Map;

/**
 *
 */
public interface Runner {

  Cancellable run(Program program, String name, Map<String, String> arguments);
}
