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

import { parseMetadata } from 'services/metadata-parser';
jest.disableAutomock();

const applicationMetadata = {
  entity: {
    details: {
      application: 'ApplicationName',
      version: '-SNAPSHOT',
    },
    type: 'application',
  },
  metadata: {
    tags: [
      {
        name: 'PurchaseHistory',
        scope: 'SYSTEM',
      },
      {
        name: 'cdap-data-pipeline',
        scope: 'SYSTEM',
      },
    ],
  },
};

describe('metadata-parser', () => {
  it('should parse application metadata', () => {
    const parsedMetadata = parseMetadata(applicationMetadata);

    expect(parsedMetadata.id).toBe('ApplicationName');
    expect(parsedMetadata.version).toBe('1.0.0-SNAPSHOT');
    expect(parsedMetadata.type).toBe('application');
  });

  it('should detect hydrator pipeline', () => {
    const parsedMetadata = parseMetadata(applicationMetadata);
    expect(parsedMetadata.isHydrator).toBe(true);
  });

  it('should parse artifact metadata', () => {
    const artifactMetadata = {
      entity: {
        details: {
          artifact: 'ArtifactName',
          namespace: 'default',
          version: '1.0.0'
        },
        type: 'artifact',
      },
    };

    const artifactParsedMetadata = parseMetadata(artifactMetadata);

    expect(artifactParsedMetadata.id).toBe('ArtifactName');
    expect(artifactParsedMetadata.version).toBe('1.0.0');
    expect(artifactParsedMetadata.type).toBe('artifact');
  });

  it('should parse dataset metadata', () => {
    const datasetMetadata = {
      entity: {
        details: {
          dataset: 'DatasetName'
        },
        type: 'dataset',
      },
    };

    const parsedMetadata = parseMetadata(datasetMetadata);

    expect(parsedMetadata.id).toBe('DatasetName');
    expect(parsedMetadata.type).toBe('dataset');
  });

  it('should parse program metadata', () => {
    const programMetadata = {
      entity: {
        details: {
          program: 'ProgramName',
          type: 'MapReduce',
          application: 'SomeApplication',
        },
        type: 'program',
      },
    };

    const parsedMetadata = parseMetadata(programMetadata);

    expect(parsedMetadata.id).toBe('ProgramName');
    expect(parsedMetadata.type).toBe('program');
    expect(parsedMetadata.applicationId).toBe('SomeApplication');
    expect(parsedMetadata.programType).toBe('MapReduce');
  });
});
