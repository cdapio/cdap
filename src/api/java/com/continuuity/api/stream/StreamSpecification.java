package com.continuuity.api.stream;



// Specification for Stream
public class StreamSpecification {

  private long ttl;
  private long maxSize;
  private String metaData;

  private void setTTL(long ttl) {
    this.ttl = ttl;
  }


  private void setMaxSize (long maxSize) {
    this.maxSize = maxSize;
  }


  private void setMetaData (String metaData ) {
    this.metaData = metaData;
   }


  //Instantiate object using Builder
  private StreamSpecification(){}


  /*
  * {@code StreamSpecification} builder used to build specification of stream
  */
  public static final class Builder {

    private long ttl = -1 ;
    private long maxSize = -1 ;
    private String metaData;

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

    public Builder setMetaData (String metaData) {
      this.metaData = metaData;
      return this;
    }


    /*
     * Create {@code StreamSpecification}
     * @return object of {@code StreamSpecification}
     */
    public StreamSpecification create() {
      StreamSpecification specification = new StreamSpecification();
      specification.setMaxSize(this.maxSize);
      specification.setMetaData(this.metaData);
      specification.setTTL(this.ttl);
      return specification;
    }

  }  

}
