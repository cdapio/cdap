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

import EntityIconMap from 'services/entity-icon-map';
import intersection from 'lodash/intersection';

export function parseMetadata(entity) {
  let type = entity.entityId.type;

  switch (type) {
    case 'artifact':
      return createArtifactObj(entity);
    case 'application':
      return createApplicationObj(entity);
    case 'datasetinstance':
      return createDatasetObj(entity);
    case 'program':
      return createProgramObj(entity);
    case 'stream':
      return createStreamObj(entity);
    case 'view':
      return createViewObj(entity);
  }
}

export function getType(entity) {
  if (entity.type === 'program') {
    return entity.programType.toLowerCase();
  } if (entity.type === 'datasetinstance') {
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
    id: entity.entityId.id.name,
    type: entity.entityId.type,
    version: entity.entityId.id.version.version,
    metadata: entity,
    scope: entity.entityId.id.namespace.id.toLowerCase() === 'system' ? 'SYSTEM' : 'USER',
    icon: EntityIconMap[entity.entityId.type]
  };
}

function createApplicationObj(entity) {
  const hydratorAritfacts = [
    'cdap-data-pipeline',
    'cdap-data-streams',
    'cdap-etl-batch',
    'cdap-etl-realtime'
  ];

  let version = entity.metadata.SYSTEM.properties.version;
  if (version === '-SNAPSHOT') {
    version = '1.0.0-SNAPSHOT';
  }

  let icon = EntityIconMap[entity.entityId.type];
  if (entity.metadata.SYSTEM.tags.indexOf('cdap-data-pipeline') !== -1) {
    icon = EntityIconMap['cdap-data-pipeline'];
  } else if (entity.metadata.SYSTEM.tags.indexOf('cdap-data-streams') !== -1) {
    icon = EntityIconMap['cdap-data-streams'];
  }

  return {
    id: entity.entityId.id.applicationId,
    type: entity.entityId.type,
    metadata: entity,
    version,
    icon,
    isHydrator: intersection(entity.metadata.SYSTEM.tags, hydratorAritfacts).length > 0
  };
}

function createDatasetObj(entity) {
  return {
    id: entity.entityId.id.instanceId,
    type: entity.entityId.type,
    metadata: entity,
    icon: EntityIconMap[entity.entityId.type]
  };
}

function createProgramObj(entity) {
  return {
    id: entity.entityId.id.id,
    applicationId: entity.entityId.id.application.applicationId,
    type: entity.entityId.type,
    programType: entity.entityId.id.type,
    metadata: entity,
    icon: EntityIconMap[entity.entityId.id.type]
  };
}

function createStreamObj(entity) {
  return {
    id: entity.entityId.id.streamName,
    type: entity.entityId.type,
    metadata: entity,
    icon: EntityIconMap[entity.entityId.type]
  };
}

function createViewObj(entity) {
  return {
    id: entity.entityId.id.id,
    type: entity.entityId.type,
    metadata: entity,
    icon: EntityIconMap[entity.entityId.type]
  };
}
