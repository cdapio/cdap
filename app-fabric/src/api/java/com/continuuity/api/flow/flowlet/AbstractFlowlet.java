/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow.flowlet;

/**
 * This abstract class provides a default implementation of {@link Flowlet} methods for easy extension.
 * It uses the result of {@link #getName()} as the Flowlet name and the result of
 * {@link #getDescription()} as the Flowlet description. By default, the {@link Class#getSimpleName()}
 * is used as the Flowlet name.
 * <p>
 *   Child classes can override the {@link #getName()} and/or {@link #getDescription()}
 *   methods to specify custom names. Children can also override the {@link #configure()} method
 *   for more control over customizing the {@link FlowletSpecification}.
 * </p>
 */
public abstract class AbstractFlowlet implements Flowlet {

  private final String name;
  private FlowletContext flowletContext;

  /**
   * Default constructor that uses {@link #getClass()}.{@link Class#getSimpleName() getSimpleName} as the
   * flowlet name.
   */
  protected AbstractFlowlet() {
    this.name = getClass().getSimpleName();
  }

  /**
   * Constructor that uses the specified name as the flowlet name.
   * @param name Name of the flowlet
   */
  protected AbstractFlowlet(String name) {
    this.name = name;
  }

  @Override
  public FlowletSpecification configure() {
    return FlowletSpecification.Builder.with()
      .setName(getName())
      .setDescription(getDescription())
      .build();
  }

  @Override
  public void initialize(FlowletContext context) throws Exception {
    this.flowletContext = context;
  }

  @Override
  public void destroy() {
    // Nothing to do.
  }

  /**
   * @return An instance of {@link FlowletContext} when this flowlet is running. Otherwise return
   *         {@code null} if it is not running or not yet initialized by the runtime environment.
   */
  protected final FlowletContext getContext() {
    return flowletContext;
  }

  /**
   * @return {@link Class#getSimpleName() Simple classname} of this {@link Flowlet}
   */
  protected String getName() {
    return name;
  }

  /**
   * @return A descriptive message about this {@link Flowlet}.
   */
  protected String getDescription() {
    return String.format("Flowlet of %s.", getName());
  }
}
