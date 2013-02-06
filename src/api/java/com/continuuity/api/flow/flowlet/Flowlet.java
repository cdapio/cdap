package com.continuuity.api.flow.flowlet;

/**
 * Defines the basic Flowlet interface. A Flowlet should provide the
 * functionality for initializing and configuring itself.
 */
public interface Flowlet {
  /**
   * Provides an interface to configure a Flowlet.
   * <p>
   *   {@link #configure()} could be possibly called multiple times and hence
   *   should not be used for allocating resources needed at run-time. This
   *   method is generally called during the configuration phase of the flowlet
   *   which happens during deployment time and few times during run-time.
   * </p>
   *
   * @return An instance of {@link FlowletSpecification}
   */
  FlowletSpecification configure();

  /**
   *  Initializes a Flowlet.
   *  <p>
   *    {@link #initialize(FlowletContext)} is called only once during the startup of
   *    a flowlet and on every instance of this flowlet.
   *  </p>
   *
   *  @param context An instance of {@link FlowletContext}
   *  @throws FlowletException
   */
  void initialize(FlowletContext context) throws FlowletException;

  /**
   * Destroy is the last thing that gets called before the flowlet is
   * shutdown. So, if there any cleanups then they can be specified here.
   *
   * <p>
   *   {@link #destroy()} will be called only when there are no more events beings processed
   *   by the flowlet.
   * </p>
   */
  void destroy();
}
