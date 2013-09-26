namespace java com.continuuity.metadata.thrift


/**
 * Thrown when there is any issue that client should know about in
 * MetadataService.
 */
exception MetadataServiceException {
  1:string message,
}
