/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.twill.TwillAppNames;
import io.cdap.cdap.master.spi.twill.ExtendedTwillApplication;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.TwillSpecification.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * The {@link TwillApplication} for running programs in distributed mode.
 */
public final class ProgramTwillApplication implements ExtendedTwillApplication {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramTwillApplication.class);

  private final ProgramRunId programRunId;
  private final ProgramOptions programOptions;
  private final TwillSpecification twillSpec;

  public ProgramTwillApplication(ProgramRunId programRunId, ProgramOptions programOptions,
                                 Map<String, RunnableDefinition> runnables,
                                 Iterable<Set<String>> launchOrder,
                                 Map<String, LocalizeResource> localizeResources,
                                 @Nullable EventHandler eventHandler) {
    this.programRunId = programRunId;
    this.programOptions = programOptions;

    // Build the TwillSpecification
    Builder.MoreRunnable moreRunnable = Builder.with()
      .setName(TwillAppNames.toTwillAppName(programRunId.getParent()))
      .withRunnable();

    // Add runnable and resources for each of them
    Builder.RunnableSetter runnableSetter = null;
    for (Map.Entry<String, RunnableDefinition> entry : runnables.entrySet()) {
      Builder.RuntimeSpecificationAdder runtimeSpecAdder = moreRunnable.add(entry.getKey(),
                                                                            entry.getValue().getRunnable(),
                                                                            entry.getValue().getResources());
      runnableSetter = localizeFiles(localizeResources, runtimeSpecAdder);
      moreRunnable = runnableSetter;
    }

    // This shouldn't happen
    Preconditions.checkState(runnableSetter != null, "No TwillRunnables for distributed program.");

    // Setup the launch order
    Iterator<Set<String>> iterator = launchOrder.iterator();
    Builder.AfterOrder afterOrder;
    if (!iterator.hasNext()) {
      afterOrder = runnableSetter.anyOrder();
    } else {
      Iterator<String> order = iterator.next().iterator();
      Preconditions.checkArgument(order.hasNext(), "Runnable launch order should have at least one runnable name");
      Builder.NextOrder nextOrder = runnableSetter.withOrder().begin(order.next(),
                                                                     Iterators.toArray(order, String.class));
      afterOrder = nextOrder;
      while (iterator.hasNext()) {
        order = iterator.next().iterator();
        Preconditions.checkArgument(order.hasNext(), "Runnable launch order should have at least one runnable name");
        nextOrder = nextOrder.nextWhenStarted(order.next(), Iterators.toArray(order, String.class));
        afterOrder = nextOrder;
      }
    }

    if (eventHandler != null) {
      twillSpec = afterOrder.withEventHandler(eventHandler).build();
    } else {
      twillSpec = afterOrder.build();
    }
  }

  /**
   * Returns the {@link ProgramRunId} of the program run represented by this {@link TwillApplication}.
   */
  public ProgramRunId getProgramRunId() {
    return programRunId;
  }

  /**
   * Returns the {@link ProgramOptions} of the program run represented by this {@link TwillApplication}.
   */
  public ProgramOptions getProgramOptions() {
    return programOptions;
  }

  @Override
  public TwillSpecification configure() {
    return twillSpec;
  }

  /**
   * Request localization of the program jar and all other files.
   */
  private Builder.RunnableSetter localizeFiles(Map<String, LocalizeResource> localizeResources,
                                               Builder.RuntimeSpecificationAdder builder) {
    Builder.LocalFileAdder fileAdder;
    Builder.MoreFile moreFile = null;
    for (Map.Entry<String, LocalizeResource> entry : localizeResources.entrySet()) {
      LOG.debug("Localizing file for {}: {} {}", programRunId, entry.getKey(), entry.getValue());
      fileAdder = (moreFile == null) ? builder.withLocalFiles() : moreFile;
      moreFile = fileAdder.add(entry.getKey(), entry.getValue().getURI(), entry.getValue().isArchive());
    }

    return moreFile == null ? builder.noLocalFiles() : moreFile.apply();
  }

  @Override
  public String getRunId() {
    return programRunId.getRun();
  }
}
