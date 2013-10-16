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
  PROCEDURE,
  MAPREDUCE,
  WORKFLOW,
  WEBAPP,
  APP,
}

/**
 * Specifies the type of a data resource.
 */
enum DataType {
  STREAM,
  DATASET,
}

/**
 * Identifies the resource that is being deployed.
 */
struct ArchiveId {
 1:required string accountId,
 2:required string applicationId,
 3:required string resource,
}

/**
 * Information about resource
 */
 struct ArchiveInfo {
  1:required string accountId,
  2:required string applicationId,
  3:required string filename,
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
}

/**
 * Following structure identifies and individual flow in the system.
 */
struct ProgramId {
 1:required string accountId,
 2:required string applicationId,
 3:required string flowId,
 4:optional EntityType type = EntityType.FLOW,
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
struct ProgramStatus {
 1:string applicationId,
 2:string flowId,
 3:RunIdentifier runId,
 4:string status,
}

/**
 * ProgramDescription include ProgramId and few more things needed to start
 * the flow. It includes parameters or arguments that will be passed around to
 * program during start.
 */
struct ProgramDescriptor {
  1:ProgramId identifier,
  2:map<string, string> arguments,
}

/**
 * Provides the state of runnables.
 */
struct ActiveProgram {
  1: string applicationId,
  2: string flowId,
  3: EntityType type,
  4: i64 lastStopped,
  5: i64 lastStarted,
  6: string currentState,
  7: i32 runs,
}

/**
 * Information returned for each Program run.
 */
struct ProgramRunRecord {
  1: string runId,
  2: i64 startTime,
  3: i64 endTime,
  4: string endStatus
}

/**
 * Schedule Id.
 */
 struct ScheduleId {
   1: string id
 }

 /**
  * Scheduled Runtime.
  */
 struct ScheduleRunTime {
   1: ScheduleId id,
   2: i64 time,
 }


/**
 * Program Service for managing flows. 
 */
service AppFabricService {

  /**
   * Starts a program
   */
  RunIdentifier start(1:AuthToken token,  2: ProgramDescriptor descriptor)
    throws (1: AppFabricServiceException e),

  /**
   * Checks the status of a program
   */
  ProgramStatus status(1:AuthToken token, 2: ProgramId identifier)
    throws (1: AppFabricServiceException e),

  /**
   * Stops a program
   */
  RunIdentifier stop(1: AuthToken token,  2: ProgramId identifier)
    throws (1: AppFabricServiceException e),

  /**
   * Set number of instance of a flowlet.
   */
  void setFlowletInstances(1: AuthToken token, 2: ProgramId identifier,
                           3: string flowletId, 4:i16 instances )
    throws (1: AppFabricServiceException e),

  /**
   * Get number of instance of a flowlet.
   */
   i32 getFlowletInstances(1: AuthToken token, 2: ProgramId identifier,
                           3: string flowletId)
     throws (1: AppFabricServiceException e),

  /**
   * Set number of instance of a program.
   */
  void setProgramInstances(1: AuthToken token, 2: ProgramId identifier,
                           3: i16 instances )
    throws (1: AppFabricServiceException e),

  /**
   * Get number of instance of a program.
   */
   i32 getProgramInstances(1: AuthToken token, 2: ProgramId identifier)
    throws (1: AppFabricServiceException e),


  /**
   * Returns the specification for a program.
   */
  string getSpecification(1: ProgramId id)
    throws (1: AppFabricServiceException e),

  /**
   * Returns all programs of a given type for an account.
   */
  string listPrograms(1: ProgramId id, 2: EntityType type)
    throws (1: AppFabricServiceException e),

  /**
   * Returns all programs of a given type for a given application.
   */
  string listProgramsByApp(1: ProgramId id, 2: EntityType type)
    throws (1: AppFabricServiceException e),

  /**
   * Returns all programs of a given type that access a stream or dataset.
   */
  string listProgramsByDataAccess(1: ProgramId id, 2: EntityType type, 3: DataType data, 4: string name)
    throws (1: AppFabricServiceException e),

  /**
   * Creates a stream.
   */
  void createStream(1: ProgramId id, 2: string spec)
    throws (1: AppFabricServiceException e),

  /**
   * Creates a dataset.
   */
  void createDataSet(1: ProgramId id, 2: string spec)
    throws (1: AppFabricServiceException e),

  /**
   * Returns the specification of a data entity (stream or dataset).
   */
  string getDataEntity(1: ProgramId id, 2: DataType type, 3: string name)
    throws (1: AppFabricServiceException e),

  /**
   * Returns the specification of a data entity (stream or dataset).
   */
  string listDataEntities(1: ProgramId id, 2: DataType type)
    throws (1: AppFabricServiceException e),

  /**
   * Returns all streams o datasets used by a given application.
   */
  string listDataEntitiesByApp(1: ProgramId id, 2: DataType type)
    throws (1: AppFabricServiceException e),

  /**
   * Returns run information for a given flow id.
   */
  list<ProgramRunRecord> getHistory(1: ProgramId id, 2: i64 startTime,
                                    3: i64 endTime, 4: i32 limit)
      throws (1: AppFabricServiceException e),

  /**
   * Returns run information for a given flow id.
   */
  void stopAll(1: string accountId)
   throws (1: AppFabricServiceException e),


  /**
   * Begins uploading of FAR
   */
  ArchiveId init(1:AuthToken token, 2:ArchiveInfo info)
    throws (1:AppFabricServiceException e),

  /**
   * Chunk of FAR is uploaded
   */
  void chunk(1:AuthToken token,
             2:ArchiveId resource, 3:binary chunk)
    throws (1: AppFabricServiceException e),

  /**
   * Finalizes uploading of FAR
   */
  void deploy(1:AuthToken token, 2:ArchiveId resource)
    throws (1: AppFabricServiceException e),

  /**
   * Status of upload
   */
  DeploymentStatus dstatus(1:AuthToken token, 2:ArchiveId resource)
    throws (1: AppFabricServiceException e),

  /**
   * Promote an application an it's resource to cloud.
   * NOTE: On this call we use overload flowid to hostname (totally wrong - but we didn't wanted to changed)
   * Javascript binding that has patching to be done. Hate Thrift.!!!!!
   */
  bool promote(1:AuthToken token, 2:ArchiveId identifier, 3:string hostname)
    throws (1: AppFabricServiceException e),

  /**
   * Deletes all Programs for a application
   * It current takes a ProgramId, but this method only interest in account id and application id.
   */
  void removeApplication(1:AuthToken token, 2:ProgramId id)
    throws (1: AppFabricServiceException e),

  /**
   * Disables all Programs for the account.
   */
  void removeAll(1:AuthToken token, 2:string accountId)
    throws (1: AppFabricServiceException e),

  /**
   * Wipes out everything for an account
   */
  void reset(1:AuthToken token, 2:string accountId)
    throws (1: AppFabricServiceException e),

  /**
   * Resume a schedule. Schedule will be resumed to run if it is not running already.
   */
   void resumeSchedule(1:AuthToken token, 2: ScheduleId identifier)
     throws (1: AppFabricServiceException e),

  /**
   * Suspend a schedule. The schedule that is running will be stopped.
   */
   void suspendSchedule(1:AuthToken token, 2: ScheduleId identifier)
    throws (1: AppFabricServiceException e),

   /**
    * Get schedules for a given program.
    */
   list<ScheduleId> getSchedules(1: AuthToken token, 2: ProgramId id)
     throws (1: AppFabricServiceException e),

   /**
    * Get next scheduled run time.
    */
    list<ScheduleRunTime> getNextScheduledRunTime(1:AuthToken token, 2: ProgramId identifier)
      throws (1: AppFabricServiceException e),

    /**
     * Store run time arguments in metadata store.
     */
    void storeRuntimeArguments(1: AuthToken token, 2: ProgramId identifier,
                               3: map<string, string> arguments)
           throws (1: AppFabricServiceException e),

    /**
     * Get the schedule state.
     */
    string getScheduleState(1: ScheduleId scheduleId)
      throws (1: AppFabricServiceException e),

    /**
     * Get runtime arguments.
     */
    map<string, string> getRuntimeArguments(1: AuthToken token, 2: ProgramId identifier)
      throws (1: AppFabricServiceException e),

}
