namespace java com.continuuity.app.services

/**
 * Delegation token is an encoded token received by the client as part of
 * consent token issues by the Overlord. It is compact, encrypted of data
 * representing the consent information provided by Overlord to
 AuthorizationService.
 */
struct AuthToken {
 1: string token,
}

/**
 * Specifies the type of resource being uploaded.
 */
enum EntityType {
  FLOW,
  QUERY,
  MAPREDUCE,
}

/**
 * Identifies the resource that is being deployed.
 */
struct ResourceIdentifier {
 1:required string accountId,
 2:required string applicationId,
 3:required string resource,
 4:required i32 version,
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
 }

/**
 * Exception raised when issues are observed during management of archive and running of applications.
 */
exception AppFabricServiceException {
  1:string message,
}

/**
 * Contains verification status of all the entities present in the deployed file.
 */
struct VerificationStatus {
  1:string applicationId,
  2:string program,
  3:i32 status,
  4:string message,
}

/**
 * Contains status of over all FAR and each flow within the FAR.
 */
struct DeploymentStatus {
  1:i32 overall,
  2:string message,
  3:list<VerificationStatus> verification,
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
  2:map<string, string> arguments,
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
service AppFabricService {

  /**
   * Starts a Flow
   */
  RunIdentifier start(1:AuthToken token,  2: FlowDescriptor descriptor)
    throws (1: AppFabricServiceException e),

  /**
   * Checks the status of a Flow
   */
  FlowStatus status(1:AuthToken token, 2: FlowIdentifier identifier)
    throws (1: AppFabricServiceException e),

  /**
   * Stops a Flow
   */
  RunIdentifier stop(1: AuthToken token,  2: FlowIdentifier identifier)
    throws (1: AppFabricServiceException e),

  /**
   * Set number of instance of a flowlet.
   */
  void setInstances(1: AuthToken token, 2: FlowIdentifier identifier,
                    3: string flowletId, 4:i16 instances )
    throws (1: AppFabricServiceException e),

  /**
   * Returns the state of flows within a given account id.
   */
  list<ActiveFlow> getFlows(1: string accountId)
     throws(1: AppFabricServiceException e),

  /**
   * Returns definition of a flow.
   */
  string getFlowDefinition(1: FlowIdentifier id)
    throws (1: AppFabricServiceException e),

  /**
   * Returns run information for a given flow id.
   */
  list<FlowRunRecord> getFlowHistory(1: FlowIdentifier id)
      throws (1: AppFabricServiceException e),

  /**
   * Returns run information for a given flow id.
   */
  void stopAll(1: string accountId)
   throws (1: AppFabricServiceException e),


  /**
   * Begins uploading of FAR
   */
  ResourceIdentifier init(1:AuthToken token, 2:ResourceInfo info)
    throws (1:AppFabricServiceException e),

  /**
   * Chunk of FAR is uploaded
   */
  void chunk(1:AuthToken token,
             2:ResourceIdentifier resource, 3:binary chunk)
    throws (1: AppFabricServiceException e),

  /**
   * Finalizes uploading of FAR
   */
  void deploy(1:AuthToken token, 2:ResourceIdentifier resource)
    throws (1: AppFabricServiceException e),

  /**
   * Status of upload
   */
  DeploymentStatus dstatus(1:AuthToken token, 2:ResourceIdentifier resource)
    throws (1: AppFabricServiceException e),

  /**
   * Promote an application an it's resource to cloud.
   * NOTE: On this call we use overload flowid to hostname (totally wrong - but we didn't wanted to changed)
   * Javascript binding that has patching to be done. Hate Thrift.!!!!!
   */
  bool promote(1:AuthToken token, 2:ResourceIdentifier identifier, 3:string hostname)
    throws (1: AppFabricServiceException e),

  /**
   * Disables a Flow or Procedure
   */
  void remove(1:AuthToken token, 2:FlowIdentifier identifier)
    throws (1: AppFabricServiceException e),

  /**
   * Deletes all Flows and Procedures of the given application.
   * It current takes a FlowIdentifier, but this method only interest in account id and application id.
   */
  void removeApplication(1:AuthToken token, 2:FlowIdentifier id)
    throws (1: AppFabricServiceException e),

  /**
   * Disables all Flows and Queries of the account
   */
  void removeAll(1:AuthToken token, 2:string accountId)
    throws (1: AppFabricServiceException e),

  /**
   * Wipes out everything for an account
   */
  void reset(1:AuthToken token, 2:string accountId)
    throws (1: AppFabricServiceException e),
}
