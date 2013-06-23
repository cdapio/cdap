namespace java com.continuuity.metadata.thrift

/**
 * Defines a account
 */
struct Account {
   1: string id,
}

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
 * Thrown when there is any issue that client should know about in
 * MetadataService.
 */
exception MetadataServiceException {
  1:string message,
}

/**
 * MetaData services
 */
service MetadataService {
  /**
   * Creates a new stream.
   *
   * @return true if successful; false otherwise
   * @throws MetadataServiceException thrown when there is issue with creating
   * stream.
   */
  bool createStream(1: Account account, 2: Stream stream)
    throws (1: MetadataServiceException e),

  /**
   * Asserts that a stream exists: creates if it does not exist, checks
   * compatibility if it already exists
   *
   * @return true if successful; false otherwise
   * @throws MetadataServiceException thrown when there is issue with creating
   * stream.
   */
  bool assertStream(1: Account account, 2: Stream stream)
    throws (1: MetadataServiceException e),

  /**
   * Deletes a stream.
   *
   * @return true if successfull; false otherwise
   * @throws MetadataServiceException thrown when there is issue with deleting
   * stream.
   */
  bool deleteStream(1: Account account, 2: Stream stream)
    throws (1: MetadataServiceException e),

  /**
   * Returns a list of streams associated with an account.
   *
   * @return list of stream associated with account.
   * @throws MetadataServiceException thrown when there is issue listing the
   * streams for an account.
   */
  list<Stream> getStreams(1: Account account)
    throws (1: MetadataServiceException e),

 /**
  * Returns a single stream with more information.
  *
  * @return information about a stream.
  * @throws MetadataServiceException thrown when there is issue reading in the
  * information for stream.
  */
  Stream getStream(1: Account account, 2: Stream stream)
    throws (1: MetadataServiceException e),

  /**
   * Creates a new dataset
   *
   * @return true if successfull; false otherwise
   * @throws MetadataServiceException thrown when there is issue with creating
   * a data set.
   */
  bool createDataset(1: Account account, 2: Dataset dataset)
    throws (1: MetadataServiceException e),

  /**
   * Asserts that a dataset exists: creates if it does not exist, checks
   * compatibility if it already exists
   *
   * @return true if successfull; false otherwise
   * @throws MetadataServiceException thrown when there is issue with creating
   * a data set.
   */
  bool assertDataset(1: Account account, 2: Dataset dataset)
    throws (1: MetadataServiceException e),

  /**
   * Deletes a dataset.
   *
   * @return true if successfull; false otherwise.
   * @throws MetadataServiceException thrown when there is issue with creating
   * a data set.
   */
  bool deleteDataset(1: Account account, 2: Dataset dataset)
    throws (1: MetadataServiceException e),

  /**
   * Returns a list of data set associated with an account.
   *
   * @return list of data set associated with account
   * @throws MetadataServiceException thrown when there is a issue with listing
   * data set.
   */
  list<Dataset> getDatasets(1: Account account)
    throws (1: MetadataServiceException e),

  /**
   * Returns a dataset.
   *
   * @return Dataset
   * @throws MetadataServiceException thrown when there is an issue with
   * retrieving the data set.
   */
  Dataset getDataset(1: Account account, 2: Dataset dataset)
    throws (1: MetadataServiceException e),

  /**
   * Creates an new application.
   *
   * @return true if created successfully or already exists, false otherwise.
   * @throws MetadataServiceException thrown when there is issue with creating
   * metadata store entry for the application.
   */
  bool createApplication(1: Account account, 2: Application application)
    throws (1: MetadataServiceException e),

  /**
   * Updates an existing application
   *
   * @return true if update was successfull, otherwise false.
   * @throws MetadataServiceException thrown when there is issue with updating
   * metadata store entry for the application.
   */
  bool updateApplication(1: Account account, 2: Application application)
    throws (1: MetadataServiceException e),

  /**
   * Deletes an application if exists.
   *
   * @return true if application was deleted successfully or did not exists to
   * be deleted; false otherwise.
   * @throws MetadataServiceException thrown when there is issue deleting an
   * application.
   */
  bool deleteApplication(1: Account account, 2: Application application)
    throws (1: MetadataServiceException e),

  /**
   * Returns a list of application associated with account.
   *
   * @returns a list of application associated with account; else empty list.
   * @throws MetadataServiceException thrown when there is issue listing
   * applications for a account.
   */
  list<Application> getApplications(1: Account account)
    throws (1: MetadataServiceException e),

 /**
  * Return more information about an application.
  *
  * @return application meta data if exists; else the id passed.
  * @throws MetadataServiceException thrown when there is issue retrieving
  * a application from metadata store.
  */
  Application getApplication(1: Account account, 2: Application application)
    throws (1: MetadataServiceException e),

  /**
   * Creates a new query.
   *
   * @return true if created successfully, false otherwise.
   * @throws MetadataServiceException thrown when there is issue with creating
   * metadata store entry for the query.
   */
  bool createQuery(1: Account account, 2: Query query)
    throws (1: MetadataServiceException e),

  /**
   * Updates a query if it exists.
   *
   * @return true if updated successfully, false otherwise.
   * @throws MetadataServiceException thrown when there is issue with creating
   * metadata store entry for the query.
   */
  bool updateQuery(1: Account account, 2: Query query)
    throws (1: MetadataServiceException e),

  /**
   * Adds a dataset to the datasets of a query if it is not there yet
   *
   * @return true if updated successfully, false otherwise.
   * @throws MetadataServiceException thrown when there is issue with updating
   * metadata store entry for the flow.
   */
   bool addDatasetToQuery(1: string account, 2: string app, 3: string query,
                          4: string dataset)
    throws (1: MetadataServiceException e),

  /**
   * Deletes an query if exists.
   *
   * @return true if query was deleted successfully or did not exists to
   * be deleted; false otherwise.
   * @throws MetadataServiceException thrown when there is issue deleting an
   * query.
   */
  bool deleteQuery(1: Account account, 2: Query query)
    throws (1: MetadataServiceException e),

  /**
   * Returns a list of query associated with account.
   *
   * @returns a list of query associated with account; else empty list.
   * @throws MetadataServiceException thrown when there is issue listing
   * query for a account.
   */
  list<Query> getQueries(1: Account account)
    throws (1: MetadataServiceException e),

 /**
  * Return more information about an query.
  *
  * @return query meta data if exists; else the id passed.
  * @throws MetadataServiceException thrown when there is issue retrieving
  * a query from metadata store.
  */
  Query getQuery(1: Account account, 2: Query query)
    throws (1: MetadataServiceException e),

  /**
   * Creates a new flow.
   *
   * @return true if created successfully or already exists, false otherwise.
   * @throws MetadataServiceException thrown when there is issue with creating
   * metadata store entry for the flow.
   */
  bool createFlow(1: string account, 2: Flow flow)
    throws (1: MetadataServiceException e),

  /**
   * Updates an existing flow
   *
   * @return true if updated successfully, false otherwise.
   * @throws MetadataServiceException thrown when there is issue with creating
   * metadata store entry for the flow.
   */
  bool updateFlow(1: string account, 2: Flow flow)
    throws (1: MetadataServiceException e),

  /**
   * Adds a dataset to the datasets of a flow if it is not there yet
   *
   * @return true if updated successfully, false otherwise.
   * @throws MetadataServiceException thrown when there is issue with updating
   * metadata store entry for the flow.
   */
   bool addDatasetToFlow(1: string account, 2: string app, 3: string flowid,
                         4: string dataset)
    throws (1: MetadataServiceException e),

  /**
   * Adds a stream to the streams of a flow if it is not there yet
   *
   * @return true if updated successfully, false otherwise.
   * @throws MetadataServiceException thrown when there is issue with updating
   * metadata store entry for the flow.
   */
   bool addStreamToFlow(1: string account, 2: string app, 3: string flowid,
                        4: string stream)
    throws (1: MetadataServiceException e),

  /**
   * Deletes an flow if exists.
   *
   * @return true if flow was deleted successfully or did not exists to
   * be deleted; false otherwise.
   * @throws MetadataServiceException thrown when there is issue deleting an
   * flow.
   */
  bool deleteFlow(1: string account, 2: string app, 3: string flowid)
    throws (1: MetadataServiceException e),

  /**
   * Returns a list of flow associated with account.
   *
   * @returns a list of flow associated with account; else empty list.
   * @throws MetadataServiceException thrown when there is issue listing
   * flow for a account.
   */
  list<Flow> getFlows(1: string account)
    throws (1: MetadataServiceException e),

 /**
  * Return more information about an flow.
  *
  * @return flow meta data if exists; else a Flow with the id and exists=false
  * @throws MetadataServiceException thrown when there is issue retrieving
  * a flow from metadata store.
  */
  Flow getFlow(1: string account, 2: string app, 3: string flowid)
    throws (1: MetadataServiceException e),

 /**
  * Return a list of all flows of an application
  *
  * @return list of all flows of the app
  * @throws MetadataServiceException thrown when there is issue retrieving
  * a flow from metadata store.
  */
  list<Flow> getFlowsByApplication(1: string account, 2: string application)
    throws (1: MetadataServiceException e),

 /**
  * Return a list of all queries of an application
  *
  * @return list of all queries of the app
  * @throws MetadataServiceException thrown when there is issue retrieving
  * a query from metadata store.
  */
  list<Query> getQueriesByApplication(1: string account, 2: string application)
    throws (1: MetadataServiceException e),

 /**
  * Return a list of all streams of an application
  *
  * @return list of all streams used by any of the app's flows
  * @throws MetadataServiceException thrown when there is issue retrieving
  * a flow from metadata store.
  */
  list<Stream> getStreamsByApplication(1: string account, 2: string application)
    throws (1: MetadataServiceException e),

 /**
  * Return a list of all datasets of an application
  *
  * @return list of all datasets used by any of the app's flows
  * @throws MetadataServiceException thrown when there is issue retrieving
  * a flow from metadata store.
  */
  list<Dataset> getDatasetsByApplication(1: string account, 2: string application)
    throws (1: MetadataServiceException e),

 /**
  * Return a list of all flows that read a stream
  *
  * @return list of all flows reading from the stream
  * @throws MetadataServiceException thrown when there is issue retrieving
  * a flow from metadata store.
  */
  list<Flow> getFlowsByStream(1: string account, 2: string stream)
    throws (1: MetadataServiceException e),

 /**
  * Return a list of all flows that use a dataset
  *
  * @return list of all flows using the dataset
  * @throws MetadataServiceException thrown when there is issue retrieving
  * a flow from metadata store.
  */
  list<Flow> getFlowsByDataset(1: string account, 2: string dataset)
    throws (1: MetadataServiceException e),

 /**
  * Return a list of all queries that use a dataset
  *
  * @return list of all queries using the dataset
  * @throws MetadataServiceException thrown when there is issue retrieving
  * a query from metadata store.
  */
  list<Query> getQueriesByDataset(1: string account, 2: string dataset)
    throws (1: MetadataServiceException e),

 /**
  * Delete all applications, flows, queries, datasets and streams for an
  * account.
  *
  * @throws MetadataServiceException thrown when there is an issue listing
  *     or deleting things.
  */
  void deleteAll(1: string account)
    throws (1: MetadataServiceException e),

}
