package com.continuuity.api.stream;

import java.util.Map;

/*
 * Specification for {@code Stream}
  */
public final class StreamSpecification {

  private long ttl;
  private long maxSize;
  private Map<String,String> metaData;
  private String name;


  private void setName(String name) {
    this.name = name;
  }

  private void setTTL(long ttl) {
    this.ttl = ttl;
  }


  private void setMaxSize (long maxSize) {
    this.maxSize = maxSize;
  }

  private void setMetaData (Map<String,String> metaData ) {
    this.metaData = metaData;
   }

  /*
   * Returns the name of the Stream
   */
  public String getName(){
    return this.name;
  }

  /*
   * Returns ttl of the Stream
   */
  public long getTTL() {
    return this.ttl;
  }


  /*
   * Returns the MaxSize of the stream
   */
  public long getMaxSize() {
    return this.maxSize;
  }


  /*
   * Returns metaData of the stream
   */
  public Map<String,String> getMetaData(){
    return this.metaData;
  }

  //Instantiate object using Builder
  private StreamSpecification(){}


  /*
  * {@code StreamSpecification} builder used to build specification of stream
  */
  public static final class Builder {

    private long ttl = -1 ;
    private long maxSize = -1 ;
    private Map<String,String> metaData;
    private String name;


    /*
     * Adds name parameter to Streams
     * @param name: stream name
     * @return Builder instance
     */
    public Builder setName(String name) {
      this.name = name;
      return this;
    }



    /*
     * Adds ttl parameter to Streams
     * @param ttl: time for streams to live -1 (default parameter) will enable to stream to live for ever
     * @return Builder instance
    */
    public Builder setTTL(long ttl) {
      this.ttl = ttl;
      return this;
    }


    /*
     *  Add maxSize to streams
     *  @param maxSize: maxSize to read from the stream -1 indicates there is no maxSize
     *  @return Builder instance
     */

    public Builder setMaxSize(long maxSize) {
      this.maxSize = maxSize;
      return this;
    }

    /*
     * Annotate metaData to Stream
     * @param metaData: MetaData to annotate the Stream
     * @return Builder instance
     */

    public Builder setMetaData (Map<String,String> metaData) {
      this.metaData = metaData;
      return this;
    }

    /*
     * Create {@code StreamSpecification}
     * @return object of {@code StreamSpecification}
     */
    public StreamSpecification create() {
      StreamSpecification specification = new StreamSpecification();
      specification.setName(name);
      specification.setMaxSize(this.maxSize);
      specification.setMetaData(this.metaData);
      specification.setTTL(this.ttl);
      return specification;
    }

  }  

}
