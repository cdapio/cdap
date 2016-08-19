/*
 * Copyright © 2015-2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.api.Resources;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.TwillSpecification.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * An abstract base class for all program {@link TwillApplication}.
 */
public abstract class AbstractProgramTwillApplication implements TwillApplication {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractProgramTwillApplication.class);

  private final Program program;
  private final Map<String, LocalizeResource> localizeResources;
  private final EventHandler eventHandler;

  /**
   * Constructor.
   *
   * @param program represents the program to be launched in Twill
   * @param localizeResources set of resources to be localized to the Twill container
   * @param eventHandler An {@link EventHandler} for the Twill application which will be set to the
   *                     Twill application specification
   */
  protected AbstractProgramTwillApplication(Program program,
                                            Map<String, LocalizeResource> localizeResources,
                                            EventHandler eventHandler) {
    this.program = program;
    this.localizeResources = ImmutableMap.copyOf(localizeResources);
    this.eventHandler = eventHandler;
    // sanity check, that should not fail, unless there's a bug elsewhere in the code
    Preconditions.checkArgument(getType().equals(program.getType()),
                                "Expected=%s, actual=%s", getType(), program.getType());
  }

  /**
   * Returns type of the program started by this {@link TwillApplication}.
   */
  protected abstract ProgramType getType();

  /**
   * Adds {@link TwillRunnable} to the application.
   *
   * @param runnables map for adding {@link TwillRunnable} and resource requirements.
   */
  protected abstract void addRunnables(Map<String, RunnableResource> runnables);

  /**
   * Applies start ordering to the runnables added to this application. By default is any order.
   */
  protected Builder.AfterOrder applyOrder(Builder.RunnableSetter runnableSetter) {
    return runnableSetter.anyOrder();
  }

  /**
   * Returns a {@link ResourceSpecification} created from the given {@link Resources} and number of instances.
   */
  protected final ResourceSpecification createResourceSpec(Resources resources, int instances) {
    return ResourceSpecification.Builder.with()
      .setVirtualCores(resources.getVirtualCores())
      .setMemory(resources.getMemoryMB(), ResourceSpecification.SizeUnit.MEGA)
      .setInstances(instances)
      .build();
  }

  @Override
  public final TwillSpecification configure() {
    Map<String, RunnableResource> runnables = Maps.newHashMap();
    addRunnables(runnables);

    Builder.MoreRunnable moreRunnable = Builder.with()
      .setName(TwillAppNames.toTwillAppName(program.getId().toEntityId()))
      .withRunnable();

    Builder.RunnableSetter runnableSetter = null;
    for (Map.Entry<String, RunnableResource> entry : runnables.entrySet()) {
      Builder.RuntimeSpecificationAdder runtimeSpecAdder = moreRunnable.add(entry.getKey(),
                                                                            entry.getValue().getRunnable(),
                                                                            entry.getValue().getResources());
      if (localizeResources.isEmpty()) {
        runnableSetter = runtimeSpecAdder.noLocalFiles();
      } else {
        runnableSetter = localizeFiles(runtimeSpecAdder.withLocalFiles());
      }
      moreRunnable = runnableSetter;
    }

    Preconditions.checkState(runnableSetter != null, "No TwillRunnables for distributed program.");
    return applyOrder(runnableSetter).withEventHandler(eventHandler).build();
  }

  /**
   * Request localization of the program jar and all other files.
   */
  private Builder.RunnableSetter localizeFiles(Builder.LocalFileAdder fileAdder) {
    // Shouldn't happen since the caller (configure()) already checked
    Preconditions.checkState(!localizeResources.isEmpty(), "Nothing to localize");

    Builder.MoreFile moreFile = null;
    for (Map.Entry<String, LocalizeResource> entry : localizeResources.entrySet()) {
      LOG.debug("Localizing file for {}: {} {} {}", program.getName(), entry.getKey(), entry.getValue());
      moreFile = fileAdder.add(entry.getKey(), entry.getValue().getURI(), entry.getValue().isArchive());
      fileAdder = moreFile;
    }
    return moreFile.apply();
  }

  /**
   * Container class for holding TwillRunnable and ResourceSpecification together.
   */
  protected static final class RunnableResource {
    private final TwillRunnable runnable;
    private final ResourceSpecification resources;

    public RunnableResource(TwillRunnable runnable, ResourceSpecification resources) {
      this.runnable = runnable;
      this.resources = resources;
    }

    public TwillRunnable getRunnable() {
      return runnable;
    }

    public ResourceSpecification getResources() {
      return resources;
    }
  }
}
