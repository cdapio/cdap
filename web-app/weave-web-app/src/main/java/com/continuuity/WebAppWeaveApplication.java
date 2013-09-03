package com.continuuity;

import com.continuuity.weave.api.WeaveApplication;
import com.continuuity.weave.api.WeaveSpecification;

import java.io.File;

/**
 *
 */
public class WebAppWeaveApplication implements WeaveApplication {
  private final File dir;
  private static final String name = "web-app";

  public WebAppWeaveApplication(final File dir) {
    this.dir = dir;
  }

  @Override
  public WeaveSpecification configure() {
    return WeaveSpecification.Builder.with()
      .setName(name)
      .withRunnable()
        .add(name, new WebAppWeaveRunnable())
      .withLocalFiles()
        .add("web-app", dir)
      .apply()
      .anyOrder()
      .build();
  }

  public static String getName() {
    return name;
  }
}
