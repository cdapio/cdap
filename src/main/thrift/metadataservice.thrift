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
   1: string id,
}

/**
 * Defines a Stream
 */
struct Stream {
   1: optional string id,
   2: optional string name,
   3: optional string description,
   4: optional map<string, string> meta,
}

/**
 * Defines a dataset
 */
struct Dataset {
   1: string id,
   2: optional string name,
   3: optional string description,
   4: optional map<string, string> meta,
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
   * @return true if successful; false otherwise
   * @throws MetadataServiceException thrown when there is issue with creating
   * stream.
   */
  bool createStream(1: Account account, 2: Stream stream)
    throws (1: MetadataServiceException e),

  /**
   * Deletes a stream.
   * @return true if successfull; false otherwise
   * @throws MetadataServiceException thrown when there is issue with deleting
   * stream.
   */
  bool deleteStream(1: Account account, 2: Stream stream)
    throws (1: MetadataServiceException e),

  /**
   * @return list of stream associated with account.
   * @throws MetadataServiceException throw when there is issue listing the
   * streams for an account.
   */
  list<Stream> getStreams(1: Account account)
    throws (1: MetadataServiceException e),

  /**
   * Creates a dataset.
   * @return true if successfull; false otherwise
   * @throws MetadataServiceException throw when there is issue with creating
   * a data set.
   */
  bool createDataset(1: Account account, 2: Dataset dataset)
    throws (1: MetadataServiceException e),

  /**
   * Deletes a dataset.
   * @return true if successfull; false otherwise.
   * @throws MetadataServiceException throw when there is issue with creating
   * a data set.
   */
  bool deleteDataset(1: Account account, 2: Dataset dataset)
    throws (1: MetadataServiceException e),

  /**
   * @return list of data set associated with account
   * @throws MetadataServiceException throw when there is issue with listing
   * data set.
   */
  list<Dataset> getDatasets(1: Account account)
    throws (1: MetadataServiceException e),
}