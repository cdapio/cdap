package com.continuuity.manager.archive;

import java.io.File;

/**
 *
 */
public interface ArchiveValidator {

  interface Status {
    String getName();

    String getMessage();

    boolean isValid();

    ArchiveType getType();
  }

  Status validate(File archive) throws IllegalArgumentException;
}
