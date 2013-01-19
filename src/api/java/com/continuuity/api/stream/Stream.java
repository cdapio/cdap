package com.continuuity.api.stream;

public interface Stream {

  /* Configures { @code Stream } by returning a {@code StreamSpecification}
  *
  * return an instance of {@code StreamSpecification }
  *
  */
  public StreamSpecification configure();

}
