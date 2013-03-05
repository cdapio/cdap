package com.continuuity.internal.test;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AuthToken;
import com.continuuity.filesystem.Location;
import com.continuuity.test.ApplicationManager;
import com.google.inject.assistedinject.Assisted;

/**
 *
 */
public interface ApplicationManagerFactory {

  ApplicationManager create(AuthToken token,
                            @Assisted("accountId") String accountId,
                            @Assisted("applicationId") String applicationId,
                            AppFabricService.Iface appFabricServer,
                            Location deployedJar,
                            ApplicationSpecification appSpec);
}
