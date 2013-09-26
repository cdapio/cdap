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
 * Defines a Flow
 */
struct Flow {
   1: required string id,
   2: required string application,
   3: optional string name,
   4: optional list<string> streams,
   5: optional list<string> datasets,
   6: optional bool exists = true,
}

/**
 * Defines a query.
 */
struct Query {
   1: required string id,
   2: required string application,
   3: optional string name,
   4: optional string description,
   5: optional string serviceName,
   6: optional list<string> datasets,
   7: optional bool exists = true,
}

/**
 * Defines a mapreduce.
 */
struct Mapreduce {
   1: required string id,
   2: required string application,
   3: optional string name,
   4: optional string description,
   6: optional list<string> datasets,
   7: optional bool exists = true,
}

/**
 * Thrown when there is any issue that client should know about in
 * MetadataService.
 */
exception MetadataServiceException {
  1:string message,
}
