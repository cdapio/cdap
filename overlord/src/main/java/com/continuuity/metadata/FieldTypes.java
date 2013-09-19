package com.continuuity.metadata;

/**
 * Defines standard fields for different types of data.
 */
public class FieldTypes {

  public static final String CREATE_DATE = "createDate";

  /**
   * Class representing constants for fields stored for Stream.
   */
  public static class Stream {
    public static final String ID = "stream";
    public static final String NAME = "name";
    public static final String DESCRIPTION = "description";
    public static final String CAPACITY_IN_BYTES = "capacityInBytes";
    public static final String EXPIRY_IN_SECONDS = "expiryInSeconds";
  }

  /**
   * Class representing constants for fields stored for Flow.
   */
  public static class Flow {
    public static final String ID = "flow";
    public static final String NAME = "name";
    public static final String STREAMS = "streams";
    public static final String DATASETS = "datasets";
  }

  /**
   * Class representing constants for fields stored for Dataset.
   */
  public static class Dataset {
    public static final String ID = "dataset";
    public static final String NAME = "name";
    public static final String DESCRIPTION = "description";
    public static final String SPECIFICATION = "specification";
    public static final String TYPE = "type";
  }

  /**
   * Class representing constants for fields stored for Application.
   */
  public static class Application {
    public static final String ID = "application";
    public static final String NAME = "name";
    public static final String DESCRIPTION = "description";
  }

  /**
   * Class representing constants for fields stored for Query.
   */
  public static class Query {
    public static final String ID = "query";
    public static final String NAME = "name";
    public static final String DATASETS = "datasets";
    public static final String DESCRIPTION = "description";
    public static final String SERVICE_NAME = "serviceName";
  }

  /**
   * Class representing constants for fields stored for Mapreduce.
   */
  public static class Mapreduce {
    public static final String ID = "mapreduce";
    public static final String NAME = "name";
    public static final String DATASETS = "datasets";
    public static final String DESCRIPTION = "description";
  }

  /**
   * Class representing constants for fields stored for Workflow.
   */
  public static class Workflow {
    public static final String ID = "workflow";
    public static final String NAME = "name";
    public static final String DESCRIPTION = "description";
  }
}
