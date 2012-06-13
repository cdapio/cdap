namespace java com.continuuity.flow.manager.stubs

/**
 * Indicates the client is not authorized to talk to FARService or 
 * FlowService. This can happen when the access has been revoked or 
 * the delegation token has been expired.
 */
exception NotAuthorizedException {
  1:string message,
}

/**
 * Exception thrown when a blocking operation timesout. Blocking 
 * operation for which a timeout is specified need a means to indicate that 
 * the timeout has occured. 
 */
exception AuthorizationTimeoutException {
  1:string message,
}

/**
 * Delegation token is an encoded token received by the client as part of consent 
 * token issues by the Overlord. It is compact, encrypted of data representing the 
 * consent information provided by Overlord to AuthorizationService.
 */
struct DelegationToken {
 1: string token,
}

/**
 * Provides authorization service. This service returns a DelegationToken that is 
 * is then used in every call to FARService or FlowService.
 */
service AuthorizationService {
  DelegationToken authenticate(1:string user, 2:string password ) 
    throws (1:NotAuthorizedException noauth, 2:AuthorizationTimeoutException authtimeout),
  DelegationToken renew(1:DelegationToken token)
    throws (1:NotAuthorizedException noauth, 2:AuthorizationTimeoutException authtimeout),
}

/**
 * Identifies the resource that is being deployed.
 */
struct ResourceIdentifier {
 1:string resource,
 2:i32 version
 3:optional string accountId = "demo",
}

/**
 * Information about resource
 */
 struct ResourceInfo {
  1:string filename,
  2:i32 size,
  3:i64 modtime,
  4:optional string accountId = "demo",
 }

/**
 * Exception raised when issues are observed during uploading of resource.
 */
exception FARServiceException {
  1:string message,
}

/**
 * Contains verification status of all the flows present in the far file.
 */
struct FlowVerificationStatus {
  1:string app,
  2:string flow,
  3:i32 status,
  4:string message,
}

/**
 * Contains status of over all FAR and each flow within the FAR.
 */
struct FARStatus {
  1:i32 overall,
  2:string message,
  3:list<FlowVerificationStatus> verification,
}

/**
 * Provides service for managing Flow Archive Resource
 */
service FARService {
  /**
   * Begins uploading of FAR
   */
  ResourceIdentifier init(1:DelegationToken token, 2: ResourceInfo info) throws (1:FARServiceException e),
  /**
   * Chunk of FAR is uploaded
   */
  void chunk(1:DelegationToken token, 2:ResourceIdentifier resource, 3: binary chunk) throws (1: FARServiceException e),
  /**
   * Finalizes uploading of FAR
   */
  void deploy(1:DelegationToken token, 2:ResourceIdentifier resource) throws (1: FARServiceException e),
  /**
   * Status of upload
   */
  FARStatus status(1:DelegationToken token, 2:ResourceIdentifier resource) throws (1: FARServiceException e),
}

/**
 * Following structure identifies and indvidual flow in the system.
 */
struct FlowIdentifier {
 1:string app,
 2:string flow,
 3:i32 version,
 4:optional string accountId = "demo",
}

/**
 * Status of a flowlet.
 */
struct FlowletStatus {
 1:string name,
 2:i32 code,
 3:string message,
}

/**
 * Run Identifier associated with flow. 
 */
struct RunIdentifier {
 1:string id,
}

/**
 * Structure specifies the return of status call for a given flow.
 */
struct FlowStatus {
 1:string appId,
 2:string flowId,
 3:i32 version,
 4:RunIdentifier runId,
 5:string status,
}

/**
 * FlowDescription include FlowIdentifier and few more things needed to start the flow. 
 * It includes parameters or arguments that will be passed around to Flow during start. 
 */
struct FlowDescriptor {
  1:FlowIdentifier identifier,
  2:list<string> arguments,
}

/**
 * Exception raised when there is any issue in start/stopping/status/pausing of a Flow. 
 */
exception FlowServiceException {
  1:string message,
}

/**
 * Flow Service for managing flows. 
 */
service FlowService {
  /**
   * Starts a Flow
   */
  RunIdentifier start(1:DelegationToken token,  2: FlowDescriptor descriptor) throws (1: FlowServiceException e),

  /**
   * Checks the status of a Flow
   */
  FlowStatus status(1:DelegationToken token, 2: FlowIdentifier identifier) throws (1: FlowServiceException e),

  /**
   * Stops a Flow
   */
  RunIdentifier stop(1: DelegationToken token,  2: FlowIdentifier identifier) throws(1: FlowServiceException e),
}