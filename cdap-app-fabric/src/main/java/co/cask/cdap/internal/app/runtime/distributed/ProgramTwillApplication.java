/*
 * Copyright © 2017 Cask Data, Inc.
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

import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.TwillSpecification.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * The {@link TwillApplication} for running programs in distributed mode.
 */
public final class ProgramTwillApplication implements TwillApplication {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramTwillApplication.class);

  private final ProgramId programId;
  private final TwillSpecification twillSpec;

  ProgramTwillApplication(ProgramId programId,
                          Map<String, RunnableDefinition> runnables,
                          Iterable<Set<String>> launchOrder,
                          Map<String, LocalizeResource> localizeResources,
                          EventHandler eventHandler) {
    this.programId = programId;

    // Build the TwillSpecification
    Builder.MoreRunnable moreRunnable = Builder.with()
      .setName(TwillAppNames.toTwillAppName(programId))
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

    twillSpec = afterOrder.withEventHandler(eventHandler).build();
  }

  /**
   * Returns the program id of the program represented by this {@link TwillApplication}.
   */
  public ProgramId getProgramId() {
    return programId;
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
      LOG.debug("Localizing file for {}: {} {}", programId, entry.getKey(), entry.getValue());
      fileAdder = (moreFile == null) ? builder.withLocalFiles() : moreFile;
      moreFile = fileAdder.add(entry.getKey(), entry.getValue().getURI(), entry.getValue().isArchive());
    }

    return moreFile == null ? builder.noLocalFiles() : moreFile.apply();
  }

}
