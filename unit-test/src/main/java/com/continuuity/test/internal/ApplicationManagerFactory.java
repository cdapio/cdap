package com.continuuity.test.internal;

import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AuthToken;
import com.continuuity.test.ApplicationManager;
import org.apache.twill.filesystem.Location;
import com.google.inject.assistedinject.Assisted;

/**
 *
 */
public interface ApplicationManagerFactory {

  ApplicationManager create(AuthToken token, @Assisted("accountId") String accountId, @Assisted("applicationId")
  String applicationId, AppFabricService.Iface appFabricServer, Location deployedJar, ApplicationSpecification appSpec);
}
