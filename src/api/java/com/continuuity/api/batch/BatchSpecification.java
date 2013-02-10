/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.batch;

/**
 * This class provides specification of a Batch. Instance of this class should be created through
 * the {@link Builder} class by invoking the {@link #builder()} method.
 */
public final class BatchSpecification {

  /**
   * Private constructor only invoked by {@link com.continuuity.api.batch.BatchSpecification.Builder#build()}
   */
  private BatchSpecification() {
  }

  /**
   * Creates an instance of {@link Builder}
   * @return An instance of {@link Builder}
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for building a {@link BatchSpecification}
   */
  public static class Builder {

    /**
     * Invoked from the static builder in {@link com.continuuity.api.batch.BatchSpecification#builder()}
     */
    private Builder() {

    }

    /**
     * @return An instance of {@link BatchSpecification}
     */
    public BatchSpecification build() {
      return new BatchSpecification();
    }
  }
}
