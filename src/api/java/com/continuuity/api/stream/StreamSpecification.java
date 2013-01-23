package com.continuuity.api.stream;

/*
 * Specification for {@code Stream}
  */
public final class StreamSpecification {

  private String name;


  private void setName(String name) {
    this.name = name;
  }

  /*
   * Returns the name of the Stream
   */
  public String getName(){
    return this.name;
  }



  //Instantiate object using Builder
  private StreamSpecification(){}


  /*
  * {@code StreamSpecification} builder used to build specification of stream
  */
  public static final class Builder {

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
     * Create {@code StreamSpecification}
     * @return object of {@code StreamSpecification}
     */
    public StreamSpecification create() {
      StreamSpecification specification = new StreamSpecification();
      specification.setName(name);
      return specification;
    }

  }  

}
