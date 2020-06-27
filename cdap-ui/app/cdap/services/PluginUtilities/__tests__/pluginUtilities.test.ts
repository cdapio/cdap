/*
 * Copyright © 2020 Cask Data, Inc.
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

import { bucketPlugins } from 'services/PluginUtilities';
jest.disableAutomock();

const pluginsResponse = [
  {
    name: 'source1',
    type: 'source',
    description: 'some source description',
    className: 'some.source.class.name',
    artifact: {
      name: 'source-plugins',
      version: '1.0.0-SNAPSHOT',
      scope: 'SYSTEM',
    },
  },
  {
    name: 'source2',
    type: 'source',
    description: 'some source description',
    className: 'some.source.class.name',
    artifact: {
      name: 'source-plugins',
      version: '1.0.0-SNAPSHOT',
      scope: 'SYSTEM',
    },
  },
  {
    name: 'source1',
    type: 'source',
    description: 'some source description',
    className: 'some.source.class.name',
    artifact: {
      name: 'source-plugins',
      version: '1.1.0-SNAPSHOT',
      scope: 'SYSTEM',
    },
  },
  {
    name: 'source1',
    type: 'source',
    description: 'some source description',
    className: 'some.source.class.name',
    artifact: {
      name: 'source-plugins',
      version: '1.1.0-SNAPSHOT',
      scope: 'USER',
    },
  },
];

describe('Plugin utilities', () => {
  it('should group the plugins with the same name', () => {
    const bucket = bucketPlugins(pluginsResponse);

    expect(Object.keys(bucket).length).toBe(2);
    expect(bucket.source1.length).toBe(3);
    expect(bucket.source2.length).toBe(1);
  });

  it('Should sort the plugins in descending order', () => {
    const bucket = bucketPlugins(pluginsResponse);

    const plugins = bucket.source1;

    expect(plugins[0].artifact.version).toBe('1.1.0-SNAPSHOT');
    expect(plugins[plugins.length - 1].artifact.version).toBe('1.0.0-SNAPSHOT');
    expect(plugins[0].artifact.scope).toBe('USER');
  });
});
