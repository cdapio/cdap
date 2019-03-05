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
import { GLOBALS } from 'services/global-constants';
import { objectQuery } from 'services/helpers';

const SYSTEM_SCOPE = 'SYSTEM';

export function parseMetadata(entityObj) {
  let type = entityObj.entity.type;

  switch (type) {
    case EntityType.artifact:
      return createArtifactObj(entityObj);
    case EntityType.application:
      return createApplicationObj(entityObj);
    case EntityType.dataset:
      return createDatasetObj(entityObj);
    case EntityType.program:
      return createProgramObj(entityObj);
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

  if (entity.metadata.metadata.tags.find((tag) => tag.name === GLOBALS.etlDataPipeline && tag.scope === SYSTEM_SCOPE)) {
    return GLOBALS.etlDataPipeline;
  } else if (entity.metadata.metadata.tags.find((tag) => tag.name === GLOBALS.etlDataStreams && tag.scope === SYSTEM_SCOPE)) {
    return GLOBALS.etlDataStreams;
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
  const datasetCount = entities.results.length - apps.length;
  return {
    pipelineCount,
    customAppCount,
    datasetCount,
  };
}

function entityIsApp(entityObj) {
  return objectQuery(entityObj, 'entity', 'type') === EntityType.application;
}

function entityIsPipeline(entityObj) {
  return (
    intersection(GLOBALS.etlPipelineTypes, objectQuery(entityObj, 'metadata', 'tags').map((tag) => tag.name))
      .length > 0
  );
}

function createArtifactObj(entityObj) {
  return {
    id: entityObj.entity.details.artifact,
    type: entityObj.entity.type.toLowerCase(),
    version: entityObj.entity.details.version,
    metadata: entityObj,
    icon: EntityIconMap['artifact'],
  };
}

function createApplicationObj(entityObj) {
  let version = entityObj.entity.details.version;
  if (version === '-SNAPSHOT') {
    version = '1.0.0-SNAPSHOT';
  }

  let icon = EntityIconMap['application'];
  if (entityObj.metadata.tags.find((tag) => tag.name === GLOBALS.etlDataPipeline && tag.scope === SYSTEM_SCOPE)) {
    icon = EntityIconMap[GLOBALS.etlDataPipeline];
  } else if (entityObj.metadata.tags.find((tag) => tag.name === GLOBALS.etlDataStreams && tag.scope === SYSTEM_SCOPE)) {
    icon = EntityIconMap[GLOBALS.etlDataStreams];
  }

  return {
    id: entityObj.entity.details.application,
    type: entityObj.entity.type.toLowerCase(),
    metadata: entityObj,
    version,
    icon,
    isHydrator: entityIsPipeline(entityObj),
  };
}

function createDatasetObj(entityObj) {
  return {
    id: entityObj.entity.details.dataset,
    type: entityObj.entity.type.toLowerCase(),
    metadata: entityObj,
    icon: EntityIconMap['dataset'],
  };
}

function createProgramObj(entityObj) {
  return {
    id: entityObj.entity.details.program,
    applicationId: entityObj.entity.details.application,
    type: entityObj.entity.type.toLowerCase(),
    programType: entityObj.entity.details.type,
    metadata: entityObj,
    icon: EntityIconMap[entityObj.entity.details.type.toLowerCase()],
  };
}
