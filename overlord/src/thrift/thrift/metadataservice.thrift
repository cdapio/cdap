namespace java com.continuuity.metadata.thrift

/**
 * Defines a application
 */
struct Application {
   1: required string id,
   2: optional string name,
   3: optional string description,
   4: optional bool exists = true,
}

/**
 * Defines a Stream
 */
struct Stream {
   1: required string id,
   2: optional string name,
   3: optional string description,
   4: optional i64 capacityInBytes,
   5: optional i64 expiryInSeconds,
   6: optional bool exists = true,
   7: optional string specification,
}

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
 * Defines a dataset
 */
struct Dataset {
   1: required string id,
   2: optional string name,
   3: optional string description,
   4: optional string type,
   5: optional bool exists = true,
   6: optional string specification,
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
