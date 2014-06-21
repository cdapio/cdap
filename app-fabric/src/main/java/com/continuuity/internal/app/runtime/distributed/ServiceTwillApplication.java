package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.api.service.ServiceSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.google.common.base.Preconditions;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.filesystem.Location;

import java.io.File;
import java.util.Map;

/**
 * TwillApplication for service. Used to localize program jar location before running the TwillApplication.
 */
public class ServiceTwillApplication implements TwillApplication {

  private final ServiceSpecification spec;
  private final Program program;
  private final File hConfig;
  private final File cConfig;
  private final EventHandler eventHandler;

  public ServiceTwillApplication(Program program, ServiceSpecification spec, File hConfig, File cConfig,
                                 EventHandler eventHandler) {
    this.program = program;
    this.spec = spec;
    this.hConfig = hConfig;
    this.cConfig = cConfig;
    this.eventHandler = eventHandler;
  }

  @Override
  public TwillSpecification configure() {
    TwillSpecification.Builder.MoreRunnable moreRunnable = TwillSpecification.Builder.with()
      .setName(String.format("%s.%s.%s.%s", Type.SERVICE.name().toLowerCase(), program.getAccountId(),
                             program.getApplicationId(), spec.getName()))
      .withRunnable();

    Location programLocation = program.getJarLocation();
    String programName = programLocation.getName();
    TwillSpecification.Builder.RunnableSetter runnableSetter = null;
    for (Map.Entry<String, RuntimeSpecification> entry : spec.getRunnables().entrySet()) {
      RuntimeSpecification runtimeSpec = entry.getValue();
      ResourceSpecification resourceSpec = runtimeSpec.getResourceSpecification();

      String runnableName = entry.getKey();
      runnableSetter = moreRunnable
        .add(runnableName, new ServiceTwillRunnable(runnableName, "hConf.xml", "cConf.xml"), resourceSpec)
        .withLocalFiles().add(programName, programLocation.toURI())
                         .add("hConf.xml", hConfig.toURI())
                         .add("cConf.xml", cConfig.toURI()).apply();
    }

    Preconditions.checkState(runnableSetter != null, "No Runnable for the Service.");
    return runnableSetter.anyOrder().withEventHandler(eventHandler).build();
  }
}
