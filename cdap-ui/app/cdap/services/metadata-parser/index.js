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

export function parseMetadata(entity) {
  let type = entity.entityId.entity;

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
  } if (entity.type === 'dataset') {
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

function createArtifactObj(entity) {
  return {
    id: entity.entityId.artifact,
    type: entity.entityId.entity.toLowerCase(),
    version: entity.entityId.version,
    metadata: entity,
    scope: entity.entityId.namespace.toLowerCase() === 'system' ? 'SYSTEM' : 'USER',
    icon: EntityIconMap['artifact']
  };
}

function createApplicationObj(entity) {
  const pipelineArtifacts = [
    'cdap-data-pipeline',
    'cdap-data-streams'
  ];

  let version = entity.entityId.version;
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
    id: entity.entityId.application,
    type: entity.entityId.entity.toLowerCase(),
    metadata: entity,
    version,
    icon,
    isHydrator: intersection(entity.metadata.SYSTEM.tags, pipelineArtifacts).length > 0
  };
}

function createDatasetObj(entity) {
  return {
    id: entity.entityId.dataset,
    type: entity.entityId.entity.toLowerCase(),
    metadata: entity,
    icon: EntityIconMap['dataset']
  };
}

function createProgramObj(entity) {
  return {
    id: entity.entityId.program,
    applicationId: entity.entityId.application,
    type: entity.entityId.entity.toLowerCase(),
    programType: entity.entityId.type,
    metadata: entity,
    icon: EntityIconMap[entity.entityId.type]
  };
}

function createStreamObj(entity) {
  return {
    id: entity.entityId.stream,
    type: entity.entityId.entity.toLowerCase(),
    metadata: entity,
    icon: EntityIconMap['stream']
  };
}

function createViewObj(entity) {
  return {
    id: entity.entityId.view,
    type: entity.entityId.entity.toLowerCase(),
    metadata: entity,
    icon: EntityIconMap['view']
  };
}
