package com.continuuity.manager.archive;

/**
 * Factory for creating {@link ArchiveValidator} based on {@link ArchiveType}.
 */
public interface ArchiveValidatorFactory {
  ArchiveValidator create(ArchiveType type);
}
