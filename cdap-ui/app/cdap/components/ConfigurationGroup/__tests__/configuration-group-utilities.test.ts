/*
 * Copyright © 2019 Cask Data, Inc.
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

import { processConfigurationGroups } from 'components/ConfigurationGroup/utilities';

const pluginProperties = {
  nonRequiredProperty: {
    name: 'nonRequiredProperty',
    macroSupported: true,
    required: false,
    type: 'string',
  },
  requiredProperty: {
    name: 'requiredProperty',
    macroSupported: false,
    required: false,
    type: 'string',
  },
  excludedProperty: {
    name: 'excludedProperty',
    macroSupported: true,
    required: true,
    type: 'string',
  },
  outputProperty: {
    name: 'outputProperty',
    macroSupported: false,
    required: true,
    type: 'string',
  },
};

const widgetJson = {
  metadata: {
    'spec-version': '1.6',
  },
  'configuration-groups': [
    {
      label: 'Required',
      descripion: 'This is a required group',
      properties: [
        {
          name: 'requiredProperty',
          label: 'Required Property',
          'widget-type': 'textbox',
          'widget-attributes': {
            default: 'required property default value',
          },
        },
      ],
    },
    {
      label: 'Non Required',
      properties: [
        {
          name: 'nonRequiredProperty',
          label: 'Non Required Property',
          'widget-type': 'csv',
          'widget-attributes': {
            default: 'non required property default value',
          },
        },
        {
          name: 'nonExistingProperty',
          label: 'Non existing property',
          'widget-type': 'textarea',
          'widget-attributes': {
            default: 'this default value should not exist',
          },
        },
      ],
    },
  ],
  outputs: [
    {
      name: 'outputProperty',
    },
  ],
};

describe('Configuration Group Parser', () => {
  it('should put all properties under Generic group when widget json does not exist', () => {
    const { defaultValues, configurationGroups } = processConfigurationGroups(
      pluginProperties,
      null,
      null
    );

    expect(defaultValues).toStrictEqual({});
    expect(configurationGroups.length).toBe(1);
    expect(configurationGroups[0].label).toBe('Generic');
    expect(configurationGroups[0].properties.length).toBe(Object.keys(pluginProperties).length);

    configurationGroups[0].properties.forEach((property) => {
      expect(property['widget-type']).toBe('textbox');
    });
  });

  it('should correctly group properties', () => {
    const { defaultValues, configurationGroups } = processConfigurationGroups(
      pluginProperties,
      widgetJson['configuration-groups'],
      widgetJson.outputs
    );

    expect(Object.keys(defaultValues).length).toBe(2);
    expect(defaultValues).toEqual(
      expect.not.objectContaining({ nonExistingProperty: 'this default value should not exist' })
    );
    expect(configurationGroups.length).toBe(3);
    expect(configurationGroups[0].label).toBe('Required');
    expect(configurationGroups[0].properties.length).toBe(1);
    expect(configurationGroups[1].properties.length).toBe(1);
    expect(configurationGroups[2].label).toBe('Generic');
    expect(configurationGroups[2].properties[0].name).toBe('excludedProperty');
    expect(configurationGroups[2].properties[0].label).toBe('excludedProperty');
    expect(configurationGroups[2].properties[0]['widget-type']).toBe('textbox');
  });
});
