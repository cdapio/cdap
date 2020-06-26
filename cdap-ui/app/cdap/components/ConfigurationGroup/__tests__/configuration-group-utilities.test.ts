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

import {
  constructErrors,
  processConfigurationGroups,
} from 'components/ConfigurationGroup/utilities';
jest.disableAutomock();

const pluginProperties = {
  nonRequiredProperty: {
    name: 'nonRequiredProperty',
    description: 'description of nonRequiredProperty',
    macroSupported: true,
    required: false,
    type: 'string',
  },
  requiredProperty: {
    name: 'requiredProperty',
    description: 'description of requiredProperty',
    macroSupported: false,
    required: false,
    type: 'string',
  },
  excludedProperty: {
    name: 'excludedProperty',
    description: 'description of excludedProperty',
    macroSupported: true,
    required: true,
    type: 'string',
  },
  outputProperty: {
    name: 'outputProperty',
    description: 'description of outputProperty',
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

const failures = [
  {
    causes: [
      {
        attributes: {
          stage: 'Stage1',
          stageConfig: 'connectionString',
        },
      },
    ],
    message: "Required property 'connectionString' has no value.",
  },
  {
    causes: [
      {
        attributes: {
          stage: 'Stage1',
          stacktrace: 'stacktrace',
        },
      },
    ],
    message: 'Orphan error message.',
  },
  {
    causes: [
      {
        attributes: {
          stage: 'Stage1',
          stageConfig: 'connectionString',
        },
      },
    ],
    message: 'Second error for connectionString. Should be discarded.',
  },
  {
    causes: [
      {
        attributes: {
          stage: 'Stage1',
          inputStage: 'source1',
          inputField: 'id',
        },
      },
    ],
    message: 'Error with input schema.',
  },
  {
    causes: [
      {
        attributes: {
          stage: 'Stage1',
          outputPort: 'source1',
          outputField: 'id',
        },
      },
    ],
    message: 'Error with output schema.',
    correctiveAction: 'Fix the error.',
  },
  {
    causes: [
      {
        attributes: {
          stage: 'Stage1',
          outputField: 'noPorts',
        },
      },
    ],
    message: 'Error with output schema when there are no ports in op schema.',
  },
  {
    causes: [
      {
        attributes: {
          stage: 'Stage1',
          configElement: 'd:c:b',
          stageConfig: 'mapping',
        },
      },
    ],
    message: "Map key 'd' must be present in the input schema.",
  },
  {
    causes: [
      {
        attributes: {
          stage: 'Stage1',
          configElement: 'd:c:b',
          stageConfig: 'mapping',
        },
      },
    ],
    message: 'Error 2 for configElement d:c:b.',
  },
  {
    causes: [
      {
        attributes: {
          stage: 'Stage1',
          configElement: 'e:b:c',
          stageConfig: 'mapping',
        },
      },
    ],
    message: "Map key 'e' must be present in the input schema.",
  },
];

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

describe('Error parser', () => {
  it('should return inputSchemaErrors, outputSchemaErrors, and propertyErrors when failures is empty', () => {
    const noFailures = [];
    const { inputSchemaErrors, outputSchemaErrors, propertyErrors } = constructErrors(noFailures);

    expect(Object.keys(inputSchemaErrors).length).toBe(0);
    expect(Object.keys(outputSchemaErrors).length).toBe(0);
    expect(Object.keys(propertyErrors).length).toBe(0);
  });
  it('should correctly parse errors and keep only first property-level and element-level error', () => {
    const { inputSchemaErrors, outputSchemaErrors, propertyErrors } = constructErrors(failures);
    expect(Object.keys(propertyErrors).length).toBe(3);
    expect(Object.keys(inputSchemaErrors).length).toBe(1);
    expect(Object.keys(outputSchemaErrors).length).toBe(2);

    expect(propertyErrors.connectionString.length).toBe(1);
    expect(propertyErrors.connectionString[0].msg).toBe(
      "Required property 'connectionString' has no value."
    );

    expect(propertyErrors.mapping.length).toBe(2);
    expect(outputSchemaErrors.source1.id).toBe(
      `${failures[4].message} ${failures[4].correctiveAction}`
    );
    expect(outputSchemaErrors.noSchemaSection.noPorts).toBe(
      'Error with output schema when there are no ports in op schema.'
    );

    expect(inputSchemaErrors.source1.id).toBe('Error with input schema.');
  });
  it('should mark errors with no affiliation as orphan errors', () => {
    const { propertyErrors } = constructErrors(failures);
    expect(propertyErrors.orphanErrors.length).toBe(1);
    expect(propertyErrors.orphanErrors[0].msg).toBe('Orphan error message.');
  });
});
