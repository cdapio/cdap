/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

import EntityIconMap from 'services/entity-icon-map';
import intersection from 'lodash/intersection';
import EntityType from 'services/metadata-parser/EntityType';
import { GLOBALS, SCOPES, SYSTEM_NAMESPACE } from 'services/global-constants';
import { objectQuery } from 'services/helpers';

export function parseMetadata(entity) {
  let type = entity.metadataEntity.type;

  switch (type) {
    case EntityType.artifact:
      return createArtifactObj(entity);
    case EntityType.application:
      return createApplicationObj(entity);
    case EntityType.dataset:
      return createDatasetObj(entity);
    case EntityType.program:
      return createProgramObj(entity);
    case EntityType.stream:
      return createStreamObj(entity);
    case EntityType.view:
      return createViewObj(entity);
  }
}

export function getType(entity) {
  if (entity.type === 'program') {
    return entity.programType.toLowerCase();
  }
  if (entity.type === 'dataset') {
    return 'dataset';
  } else if (entity.type !== 'application') {
    return entity.type;
  }

  if (entity.metadata.metadata.SYSTEM.tags.indexOf('cdap-data-pipeline') !== -1) {
    return 'cdap-data-pipeline';
  } else if (entity.metadata.metadata.SYSTEM.tags.indexOf('cdap-data-streams') !== -1) {
    return 'cdap-data-streams';
  } else {
    return entity.type;
  }
}

/**
 * TODO:
 * This function should be refactored. It should just return the entities count, and not care about the entity types.
 *
 * https://issues.cask.co/browse/CDAP-14639
 */
export function getCustomAppPipelineDatasetCounts(entities) {
  const apps = entities.results.filter((entity) => entityIsApp(entity));
  const pipelineCount = apps.filter((entity) => entityIsPipeline(entity)).length;
  const customAppCount = apps.length - pipelineCount;
  const datasetCount = entities.total - apps.length;
  return {
    pipelineCount,
    customAppCount,
    datasetCount,
  };
}

function entityIsApp(entity) {
  return objectQuery(entity, 'metadataEntity', 'type') === EntityType.application;
}

function entityIsPipeline(entity) {
  return (
    intersection(GLOBALS.etlPipelineTypes, objectQuery(entity, 'metadata', SCOPES.SYSTEM, 'tags'))
      .length > 0
  );
}

function createArtifactObj(entity) {
  return {
    id: entity.metadataEntity.details.artifact,
    type: entity.metadataEntity.type.toLowerCase(),
    version: entity.metadataEntity.details.version,
    metadata: entity,
    scope:
      entity.metadataEntity.details.namespace.toLowerCase() === SYSTEM_NAMESPACE ? SCOPES.SYSTEM : SCOPES.USER,
    icon: EntityIconMap['artifact'],
  };
}

function createApplicationObj(entity) {
  let version = entity.metadataEntity.details.version;
  if (version === '-SNAPSHOT') {
    version = '1.0.0-SNAPSHOT';
  }

  let icon = EntityIconMap['application'];
  if (entity.metadata.SYSTEM.tags.indexOf('cdap-data-pipeline') !== -1) {
    icon = EntityIconMap['cdap-data-pipeline'];
  } else if (entity.metadata.SYSTEM.tags.indexOf('cdap-data-streams') !== -1) {
    icon = EntityIconMap['cdap-data-streams'];
  }

  return {
    id: entity.metadataEntity.details.application,
    type: entity.metadataEntity.type.toLowerCase(),
    metadata: entity,
    version,
    icon,
    isHydrator: entityIsPipeline(entity),
  };
}

function createDatasetObj(entity) {
  return {
    id: entity.metadataEntity.details.dataset,
    type: entity.metadataEntity.type.toLowerCase(),
    metadata: entity,
    icon: EntityIconMap['dataset'],
  };
}

function createProgramObj(entity) {
  return {
    id: entity.metadataEntity.details.program,
    applicationId: entity.metadataEntity.details.application,
    type: entity.metadataEntity.type.toLowerCase(),
    programType: entity.metadataEntity.details.type,
    metadata: entity,
    icon: EntityIconMap[entity.metadataEntity.details.type],
  };
}

function createStreamObj(entity) {
  return {
    id: entity.metadataEntity.details.stream,
    type: entity.metadataEntity.type.toLowerCase(),
    metadata: entity,
    icon: EntityIconMap['stream'],
  };
}

function createViewObj(entity) {
  return {
    id: entity.metadataEntity.details.view,
    type: entity.metadataEntity.type.toLowerCase(),
    metadata: entity,
    icon: EntityIconMap['view'],
  };
}
