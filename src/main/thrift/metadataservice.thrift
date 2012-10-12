namespace java com.continuuity.metadata.stubs

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
}

/**
 * Defines a dataset types.
 */
enum DatasetType {
  BASIC,
  COUNTER,
  TIME_SERIES,
  CSV
}

/**
 * Defines a dataset
 */
struct Dataset {
   1: required string id,
   2: optional string name,
   3: optional string description,
   4: optional DatasetType type,
}

/**
 * Defines a query dataset.
 */
struct Query {
   1: required string id,
   2: optional string name,
   3: optional string description,
   4: optional string serviceName,
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
   * Creates a stream.
   *
   * @return true if successful; false otherwise
   * @throws MetadataServiceException thrown when there is issue with creating
   * stream.
   */
  bool createStream(1: Account account, 2: Stream stream)
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
  * Retruns a single stream with more information.
  *
  * @return information about a stream.
  * @throws MetadataServiceException thrown when there is issue reading in the
  * information for stream.
  */
  Stream getStream(1: Account account, 2: Stream stream)
    throws (1: MetadataServiceException e),

  /**
   * Creates a dataset.
   *
   * @return true if successfull; false otherwise
   * @throws MetadataServiceException thrown when there is issue with creating
   * a data set.
   */
  bool createDataset(1: Account account, 2: Dataset dataset)
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
   * Creates an application if not exists.
   *
   * @return true if created successfully or already exists, false otherwise.
   * @throws MetadataServiceException thrown when there is issue with creating
   * metadata store entry for the application.
   */
  bool createApplication(1: Account account, 2: Application application)
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
   * Creates an query if not exists.
   *
   * @return true if created successfully or already exists, false otherwise.
   * @throws MetadataServiceException thrown when there is issue with creating
   * metadata store entry for the query.
   */
  bool createQuery(1: Account account, 2: Query query)
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
}