namespace java com.continuuity.metadata.thrift

/**
 * Defines a workflow
 */
struct Workflow {
   1: required string id,
   2: required string application,
   3: optional string name,
   4: optional bool exists = true,
}

/**
 * Thrown when there is any issue that client should know about in
 * MetadataService.
 */
exception MetadataServiceException {
  1:string message,
}
