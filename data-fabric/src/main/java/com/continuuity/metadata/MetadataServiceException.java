
package com.continuuity.metadata;

/**
 * Thrown when there is any issue that client should know about in
 * MetaDataStore.
 */
public class MetadataServiceException extends Exception {
  public MetadataServiceException(String message) {
    super(message);
  }
}

