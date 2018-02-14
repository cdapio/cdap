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

import {parseMetadata} from 'services/metadata-parser';

describe('metadata-parser', () => {
  it('should parse application metadata', () => {
    const applicationMetadata = {
      entityId: {
        entity: 'APPLICATION',
        application: 'ApplicationName',
        version: '-SNAPSHOT'
      },
      metadata: {
        SYSTEM: {
          tags: ['PurchaseHistory']
        }
      }
    };

    const parsedMetadata = parseMetadata(applicationMetadata);

    expect(parsedMetadata.id).toBe('ApplicationName');
    expect(parsedMetadata.version).toBe('1.0.0-SNAPSHOT');
    expect(parsedMetadata.type).toBe('application');
    expect(parsedMetadata.isHydrator).toBe(false);
  });

  it('should detect hydrator pipeline', () => {
    const applicationMetadata = {
      entityId: {
        entity: 'APPLICATION',
        application: 'ApplicationName',
        version: '-SNAPSHOT'
      },
      metadata: {
        SYSTEM: {
          tags: ['cdap-data-pipeline']
        }
      }
    };

    const parsedMetadata = parseMetadata(applicationMetadata);

    expect(parsedMetadata.isHydrator).toBe(true);
  });

  it('should parse artifact metadata', () => {
    const systemArtifactMetadata = {
      entityId: {
        entity: 'ARTIFACT',
        artifact: 'ArtifactName',
        namespace: 'SYSTEM',
        version: '1.0.0'
      }
    };

    const userArtifactMetadata = {
      entityId: {
        entity: 'ARTIFACT',
        artifact: 'ArtifactName',
        namespace: 'USER',
        version: '1.0.0'
      }
    };

    const systemParsedMetadata = parseMetadata(systemArtifactMetadata);
    const userParsedMetadata = parseMetadata(userArtifactMetadata);

    expect(systemParsedMetadata.id).toBe('ArtifactName');
    expect(systemParsedMetadata.version).toBe('1.0.0');
    expect(systemParsedMetadata.type).toBe('artifact');
    expect(systemParsedMetadata.scope).toBe('SYSTEM');
    expect(userParsedMetadata.scope).toBe('USER');

  });

  it('should parse dataset metadata', () => {
    const datasetMetadata = {
      entityId: {
        entity: 'DATASET',
        dataset: 'DatasetName'
      }
    };

    const parsedMetadata = parseMetadata(datasetMetadata);

    expect(parsedMetadata.id).toBe('DatasetName');
    expect(parsedMetadata.type).toBe('dataset');
  });

  it('should parse stream metadata', () => {
    const streamMetadata = {
      entityId: {
        entity: 'STREAM',
        stream: 'StreamName'
      }
    };

    const parsedMetadata = parseMetadata(streamMetadata);

    expect(parsedMetadata.id).toBe('StreamName');
    expect(parsedMetadata.type).toBe('stream');
  });

  it('should parse program metadata', () => {
    const programMetadata = {
      entityId: {
        entity: 'PROGRAM',
        program: 'ProgramName',
        type: 'Flow',
        application: 'SomeApplication'
      }
    };

    const parsedMetadata = parseMetadata(programMetadata);

    expect(parsedMetadata.id).toBe('ProgramName');
    expect(parsedMetadata.type).toBe('program');
    expect(parsedMetadata.applicationId).toBe('SomeApplication');
    expect(parsedMetadata.programType).toBe('Flow');
  });

  it('should parse view metadata', () => {
    const viewMetadata = {
      entityId: {
        entity: 'VIEW',
        view: 'ViewName'
      }
    };

    const parsedMetadata = parseMetadata(viewMetadata);

    expect(parsedMetadata.id).toBe('ViewName');
    expect(parsedMetadata.type).toBe('view');

  });
});
