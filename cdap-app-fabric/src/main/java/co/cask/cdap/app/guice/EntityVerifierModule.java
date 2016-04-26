/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.guice;

import co.cask.cdap.common.entity.DefaultEntityExistenceVerifier;
import co.cask.cdap.common.entity.EntityExistenceVerifier;
import co.cask.cdap.common.entity.InstanceExistenceVerifier;
import co.cask.cdap.data.stream.StreamExistenceVerifier;
import co.cask.cdap.data.view.ViewExistenceVerifier;
import co.cask.cdap.data2.dataset2.DatasetExistenceVerifier;
import co.cask.cdap.internal.app.namespace.NamespaceExistenceVerifier;
import co.cask.cdap.internal.app.runtime.ApplicationExistenceVerifier;
import co.cask.cdap.internal.app.runtime.ProgramExistenceVerifier;
import co.cask.cdap.internal.app.runtime.ProgramRunExistenceVerifier;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactExistenceVerifier;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.InstanceId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;

/**
 * {@link PrivateModule} for {@link EntityExistenceVerifier} bindings.
 */
public class EntityVerifierModule extends PrivateModule {
  @Override
  protected void configure() {
    MapBinder<Class<? extends EntityId>, EntityExistenceVerifier<? extends EntityId>> existenceVerifiers =
      MapBinder.newMapBinder(binder(),
                             new TypeLiteral<Class<? extends EntityId>>() { },
                             new TypeLiteral<EntityExistenceVerifier<? extends EntityId>>() { });

    existenceVerifiers.addBinding(InstanceId.class).to(InstanceExistenceVerifier.class);
    existenceVerifiers.addBinding(NamespaceId.class).to(NamespaceExistenceVerifier.class);
    existenceVerifiers.addBinding(ArtifactId.class).to(ArtifactExistenceVerifier.class);
    existenceVerifiers.addBinding(ApplicationId.class).to(ApplicationExistenceVerifier.class);
    existenceVerifiers.addBinding(ProgramId.class).to(ProgramExistenceVerifier.class);
    existenceVerifiers.addBinding(ProgramRunId.class).to(ProgramRunExistenceVerifier.class);
    existenceVerifiers.addBinding(StreamId.class).to(StreamExistenceVerifier.class);
    existenceVerifiers.addBinding(StreamViewId.class).to(ViewExistenceVerifier.class);
    existenceVerifiers.addBinding(DatasetId.class).to(DatasetExistenceVerifier.class);

    bind(EntityExistenceVerifier.class).to(DefaultEntityExistenceVerifier.class);
    expose(EntityExistenceVerifier.class);
  }
}
