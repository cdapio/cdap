package com.continuuity.test.internal;

import com.continuuity.app.ApplicationSpecification;
import com.continuuity.test.ApplicationManager;
import com.google.inject.assistedinject.Assisted;
import org.apache.twill.filesystem.Location;

/**
 *
 */
public interface ApplicationManagerFactory {

  ApplicationManager create(@Assisted("accountId") String accountId, @Assisted("applicationId") String applicationId,
                            Location deployedJar, ApplicationSpecification appSpec);
}
