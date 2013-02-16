namespace java com.continuuity.app.services

/**
 * Indicates the client is not authorized to talk to FARService or 
 * FlowService. This can happen when the access has been revoked or 
 * the delegation token has been expired.
 */
exception NotAuthorizedException {
  1:string message,
}

/**
 * Exception thrown when a blocking operation times out. Blocking
 * operation for which a timeout is specified need a means to indicate that 
 * the timeout has occurred.
 */
exception AuthorizationTimeoutException {
  1:string message,
}

/**
 * Delegation token is an encoded token received by the client as part of
 * consent token issues by the Overlord. It is compact, encrypted of data
 * representing the consent information provided by Overlord to
 AuthorizationService.
 */
struct DelegationToken {
 1: string token,
}

/**
 * Provides authorization service. This service returns a DelegationToken that
 * is then used in every call to FARService or FlowService.
 */
service AuthorizationService {
  DelegationToken authenticate(1:string user, 2:string password ) 
    throws (1:NotAuthorizedException noauth,
            2:AuthorizationTimeoutException authtimeout),
  DelegationToken renew(1:DelegationToken token)
    throws (1:NotAuthorizedException noauth,
            2:AuthorizationTimeoutException authtimeout),
}

/**
 * Specifies the type of resource being uploaded.
 */
enum EntityType {
  FLOW,
  QUERY,
}

/**
 * Identifies the resource that is being deployed.
 */
struct ResourceIdentifier {
 1:required string accountId,
 2:required string applicationId,
 3:required string resource,
 4:required i32 version,
 5:optional EntityType type = EntityType.FLOW,
}

/**
 * Information about resource
 */
 struct ResourceInfo {
  1:required string accountId,
  2:required string applicationId,
  3:required string filename,
  4:required i32 size,
  5:required i64 modtime,
  6:optional EntityType type = EntityType.FLOW,
 }

/**
 * Exception raised when issues are observed during uploading of resource.
 */
exception FARServiceException {
  1:string message,
}

/**
 * Contains verification status of all the entities present in the far file.
 */
struct FlowVerificationStatus {
  1:string applicationId,
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
 * Following structure identifies and individual flow in the system.
 */
struct FlowIdentifier {
 1:required string accountId,
 2:required string applicationId,
 3:required string flowId,
 4:required i32 version = -1,
 5:optional EntityType type = EntityType.FLOW,
}

/**
 * Provides service for managing Flow Archive Resource
 */
service FARService {

  /**
   * Begins uploading of FAR
   */
  ResourceIdentifier init(1:DelegationToken token, 2:ResourceInfo info)
    throws (1:FARServiceException e),

  /**
   * Chunk of FAR is uploaded
   */
  void chunk(1:DelegationToken token,
             2:ResourceIdentifier resource, 3:binary chunk)
    throws (1: FARServiceException e),

  /**
   * Finalizes uploading of FAR
   */
  void deploy(1:DelegationToken token, 2:ResourceIdentifier resource)
    throws (1: FARServiceException e),

  /**
   * Status of upload
   */
  FARStatus status(1:DelegationToken token, 2:ResourceIdentifier resource)
    throws (1: FARServiceException e),

  /**
   * Promote a flow an it's resource to cloud.
   */
  bool promote(1:DelegationToken token, 2:FlowIdentifier identifier)
    throws (1: FARServiceException e),

  /**
   * Disables a Flow
   */
  void remove(1:DelegationToken token, 2:FlowIdentifier identifier)
    throws (1: FARServiceException e),

  /**
   * Disables all Flows and Queries
   */
  void removeAll(1:DelegationToken token, 2:string accountId)
    throws (1: FARServiceException e),

  /**
   * Wipes out everything for an account
   */
  void reset(1:DelegationToken token, 2:string accountId)
    throws (1: FARServiceException e),

}

/**
 * Exception raised when there is an issue in start/stop/status/pausing of Flows
 */
exception FlowServiceException {
  1:string message,
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
 1:string applicationId,
 2:string flowId,
 3:i32 version,
 4:RunIdentifier runId,
 5:string status,
}

/**
 * FlowDescription include FlowIdentifier and few more things needed to start
 * the flow. It includes parameters or arguments that will be passed around to
 * Flow during start.
 */
struct FlowDescriptor {
  1:FlowIdentifier identifier,
  2:list<string> arguments,
}

/**
 * Provides the state of flows.
 */
struct ActiveFlow {
  1: string applicationId,
  2: string flowId,
  3: EntityType type,
  4: i64 lastStopped,
  5: i64 lastStarted,
  6: string currentState,
  7: i32 runs,
}

/**
 * Information returned for each Flow run.
 */
struct FlowRunRecord {
  1: string runId,
  2: i64 startTime,
  3: i64 endTime,
  4: string endStatus
}

/**
 * Flow Service for managing flows. 
 */
service FlowService {

  /**
   * Starts a Flow
   */
  RunIdentifier start(1:DelegationToken token,  2: FlowDescriptor descriptor)
    throws (1: FlowServiceException e),

  /**
   * Checks the status of a Flow
   */
  FlowStatus status(1:DelegationToken token, 2: FlowIdentifier identifier)
    throws (1: FlowServiceException e),

  /**
   * Stops a Flow
   */
  RunIdentifier stop(1: DelegationToken token,  2: FlowIdentifier identifier)
    throws (1: FlowServiceException e),

  /**
   * Set number of instance of a flowlet.
   */
  void setInstances(1: DelegationToken token, 2: FlowIdentifier identifier,
                    3: string flowletId, 4:i16 instances )
    throws (1: FlowServiceException e),

  /**
   * Returns the state of flows within a given account id.
   */
  list<ActiveFlow> getFlows(1: string accountId)
     throws(1: FlowServiceException e),

  /**
   * Returns definition of a flow.
   */
  string getFlowDefinition(1: FlowIdentifier id)
    throws (1: FlowServiceException e),

  /**
   * Returns run information for a given flow id.
   */
  list<FlowRunRecord> getFlowHistory(1: FlowIdentifier id),

  /**
   * Returns run information for a given flow id.
   */
  void stopAll(1: string accountId)
   throws (1: FlowServiceException e),
}
