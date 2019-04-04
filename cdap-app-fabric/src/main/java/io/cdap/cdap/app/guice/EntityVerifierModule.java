/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.app.guice;

import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import io.cdap.cdap.common.entity.DefaultEntityExistenceVerifier;
import io.cdap.cdap.common.entity.EntityExistenceVerifier;
import io.cdap.cdap.common.entity.InstanceExistenceVerifier;
import io.cdap.cdap.data2.dataset2.DatasetExistenceVerifier;
import io.cdap.cdap.internal.app.namespace.NamespaceExistenceVerifier;
import io.cdap.cdap.internal.app.runtime.ApplicationExistenceVerifier;
import io.cdap.cdap.internal.app.runtime.ProgramExistenceVerifier;
import io.cdap.cdap.internal.app.runtime.ProgramRunExistenceVerifier;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactExistenceVerifier;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;

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
    existenceVerifiers.addBinding(DatasetId.class).to(DatasetExistenceVerifier.class);

    TypeLiteral<EntityExistenceVerifier<EntityId>> verifierType =
      new TypeLiteral<EntityExistenceVerifier<EntityId>>() { };
    bind(verifierType).to(DefaultEntityExistenceVerifier.class);
    expose(verifierType);
  }
}
