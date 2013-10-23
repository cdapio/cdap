package com.continuuity.internal.app.services;

import com.continuuity.app.services.AppFabricService;
import com.continuuity.internal.app.runtime.schedule.Scheduler;

/**
 * An assisted inject factory for creating AppFabricService.Iface so that we can provide a Scheduler.
 */
public interface AppFabricServiceFactory {

  /**
   * Create appfabric service instance.
   *
   * @param scheduler an instance of {@link Scheduler}.
   * @return          an instance of {@link AppFabricService.Iface}.
   */
  AppFabricService.Iface create(Scheduler scheduler);
}
