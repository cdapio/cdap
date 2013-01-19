package com.continuuity.api.stream;


// Specification for Stream
public class StreamSpecification {

  //Instantiate object using Builder
  private StreamSpecification(){}

  /*
  * {@code StreamSpecification} builder used to build specification of stream
  */
  public static final class Builder {

    public StreamSpecification create() {
      StreamSpecification specification = new StreamSpecification();
      return specification;
    }

  }  

}
