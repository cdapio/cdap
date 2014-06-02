/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.zookeeper.coordination;

import com.google.common.base.Function;

import javax.annotation.Nullable;

/**
 * A function to modify a {@link ResourceRequirement}.
 */
public interface ResourceModifier extends Function<ResourceRequirement, ResourceRequirement> {

  /**
   * Called to produce a new {@link ResourceRequirement}.
   *
   * @param existingRequirement The existing requirement or {@code null} if no existing requirement
   * @return The new requirement to submit or {@code null} if no modification wanted to be made
   */
  @Nullable
  @Override
  ResourceRequirement apply(@Nullable ResourceRequirement existingRequirement);
}
