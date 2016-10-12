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

const iconMap = {
  application: 'icon-fist',
  artifact: 'fa fa-archive',
  'cdap-data-pipeline': 'icon-ETLBatch',
  'cdap-data-streams': 'icon-sparkstreaming',
  datasetinstance: 'icon-datasets',
  stream: 'icon-streams',
  view: 'icon-streamview',
  'Workflow': 'icon-workflow',
  'Spark': 'icon-spark',
  'Mapreduce': 'icon-mapreduce',
  'Service': 'icon-service',
  'Worker': 'icon-worker',
  'Flow': 'icon-tigon'
};

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

function createArtifactObj(entity) {
  return {
    id: entity.entityId.id.name,
    type: entity.entityId.type,
    version: entity.entityId.id.version.version,
    metadata: entity,
    scope: entity.entityId.id.namespace.id.toLowerCase() === 'system' ? 'SYSTEM' : 'USER',
    icon: iconMap[entity.entityId.type]
  };
}

function createApplicationObj(entity) {
  return {
    id: entity.entityId.id.applicationId,
    type: entity.entityId.type,
    metadata: entity,
    version: `1.0.0${entity.metadata.SYSTEM.properties.version}`,
    icon: iconMap[entity.entityId.type]
  };
}

function createDatasetObj(entity) {
  return {
    id: entity.entityId.id.instanceId,
    type: entity.entityId.type,
    metadata: entity,
    icon: iconMap[entity.entityId.type]
  };
}

function createProgramObj(entity) {
  return {
    id: entity.entityId.id.id,
    applicationId: entity.entityId.id.application.applicationId,
    type: entity.entityId.type,
    programType: entity.entityId.id.type,
    metadata: entity,
    icon: iconMap[entity.entityId.id.type]
  };
}

function createStreamObj(entity) {
  return {
    id: entity.entityId.id.streamName,
    type: entity.entityId.type,
    metadata: entity,
    icon: iconMap[entity.entityId.type]
  };
}

function createViewObj(entity) {
  return {
    id: entity.entityId.id.id,
    type: entity.entityId.type,
    metadata: entity,
    icon: iconMap[entity.entityId.type]
  };
}
