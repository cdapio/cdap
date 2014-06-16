package com.continuuity.api.data.stream;

/**
 * Specification for {@link Stream}.
 */
public final class StreamSpecification {
  private final String name;

  private StreamSpecification(final String name) {
    this.name = name;
  }

  /**
   * Returns the name of the Stream.
   */
  public String getName() {
    return name;
  }

 /**
  * {@code StreamSpecification} builder used to build specification of stream.
  */
  public static final class Builder {
    private String name;

    /**
     * Adds name parameter to Streams.
     * @param name stream name
     * @return Builder instance
     */
    public Builder setName(final String name) {
      this.name = name;
      return this;
    }

    /**
     * Create {@code StreamSpecification}.
     * @return Instance of {@code StreamSpecification}
     */
    public StreamSpecification create() {
      StreamSpecification specification = new StreamSpecification(name);
      return specification;
    }
  }
}
