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
  CustomOperator,
  IWidgetJson,
  IWidgetProperty,
  PropertyShowConfigTypeEnums,
} from 'components/ConfigurationGroup/types';
import { processConfigurationGroups } from 'components/ConfigurationGroup/utilities';
import {
  filterByCondition,
  IFilteredWidgetProperty,
} from 'components/ConfigurationGroup/utilities/DynamicPluginFilters';
jest.disableAutomock();

const pluginProperties = {
  property1: {
    name: 'property1',
    description: 'property1',
    macroSupported: true,
    required: false,
    type: 'string',
  },
  property2: {
    name: 'property2',
    description: 'property2',
    macroSupported: true,
    required: false,
    type: 'string',
  },
  property3: {
    name: 'property3',
    description: 'property4',
    macroSupported: true,
    required: false,
    type: 'string',
  },
  property4: {
    name: 'property4',
    description: 'property4',
    macroSupported: true,
    required: false,
    type: 'string',
  },
  property5: {
    name: 'property5',
    description: 'property5',
    macroSupported: true,
    required: false,
    type: 'int',
  },
  property6: {
    name: 'property6',
    description: 'property6',
    macroSupported: true,
    required: false,
    type: 'string',
  },
  property7: {
    name: 'property7',
    description: 'property7',
    macroSupported: true,
    required: false,
    type: 'string',
  },
  property8: {
    name: 'property8',
    description: 'property8',
    macroSupported: true,
    required: false,
    type: 'string',
  },
  property9: {
    name: 'property9',
    description: 'property9',
    macroSupported: true,
    required: false,
    type: 'string',
  },
  property10: {
    name: 'property10',
    description: 'property10',
    macroSupported: true,
    required: false,
    type: 'string',
  },
  property11: {
    name: 'property11',
    description: 'property11',
    macroSupported: true,
    required: false,
    type: 'string',
  },
  property12: {
    name: 'property12',
    description: 'property12',
    macroSupported: true,
    required: false,
    type: 'string',
  },
  property13: {
    name: 'property13',
    description: 'property13',
    macroSupported: true,
    required: false,
    type: 'string',
  },
  property14: {
    name: 'property14',
    description: 'property14',
    macroSupported: true,
    required: false,
    type: 'string',
  },
  property15: {
    name: 'property15',
    description: 'property15',
    macroSupported: true,
    required: false,
    type: 'string',
  },
  property16: {
    name: 'property16',
    description: 'property16',
    macroSupported: true,
    required: false,
    type: 'string',
  },
  property17: {
    name: 'property17',
    description: 'property17',
    macroSupported: true,
    required: false,
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
const outputProperty: IWidgetProperty[] = [
  {
    name: 'outputProperty',
  },
];
const widgetJson: IWidgetJson = {
  'configuration-groups': [
    {
      label: 'Group with filters',
      description: 'Group with properties to test filters',
      properties: [
        {
          name: 'property1',
          label: 'Property 1',
          'widget-type': 'radio-group',
          'widget-attributes': {
            default: 'true',
          },
        },
        {
          name: 'property2',
          label: 'Property 2',
          'widget-type': 'textbox',
          'widget-attributes': {
            default: 'value for property2',
          },
        },
        {
          name: 'property3',
          label: 'Property 3',
          'widget-type': 'textbox',
          'widget-attributes': {
            default: 'value for property3',
          },
        },
        {
          name: 'property4',
          label: 'Property 4',
          'widget-type': 'textbox',
          'widget-attributes': {
            default: 'value for property4',
          },
        },
      ],
    },
    {
      label: 'Group with complex filters',
      description: 'Group with properties to test complex filters',
      properties: [
        {
          name: 'property5',
          label: 'Property 5',
          'widget-type': 'number',
          'widget-attributes': {
            default: '1',
          },
        },
        {
          name: 'property6',
          label: 'Property 6',
          'widget-type': 'select',
          'widget-attributes': {
            options: ['VALUE1', 'VALUE2', 'VALUE3'],
            default: 'VALUE1',
          },
        },
        {
          name: 'property7',
          label: 'property7',
          'widget-type': 'textbox',
          'widget-attributes': {
            default: 'property7',
          },
        },
        {
          name: 'property8',
          label: 'property8',
          'widget-type': 'textbox',
          'widget-attributes': {
            default: 'property8',
          },
        },
        {
          name: 'property9',
          label: 'property9',
          'widget-type': 'textbox',
          'widget-attributes': {
            default: 'property9',
          },
        },
      ],
    },
    {
      label: 'Group with simple condition object',
      description: 'Group with properties to test simple condition object',
      properties: [
        {
          name: 'property10',
          label: 'Property 10',
          'widget-type': 'textbox',
          'widget-attributes': {
            default: 'property10',
          },
        },
        {
          name: 'property11',
          label: 'Property 11',
          'widget-type': 'textbox',
          'widget-attributes': {
            default: 'property11',
          },
        },
        {
          name: 'property12',
          label: 'Property 12',
          'widget-type': 'textbox',
        },
        {
          name: 'property13',
          label: 'Property 13',
          'widget-type': 'textbox',
          'widget-attributes': {
            default: 'property13',
          },
        },
        {
          name: 'property14',
          label: 'Property 14',
          'widget-type': 'number',
          'widget-attributes': {
            default: '10',
          },
        },
        {
          name: 'property15',
          label: 'Property 15',
          'widget-type': 'number',
          'widget-attributes': {
            default: 'property15',
          },
        },
      ],
    },
    {
      label: 'Hide entire group',
      description: 'Group to be hidden entirely if propety1 is Inifinity',
      properties: [
        {
          name: 'property16',
          label: 'Property 16',
          'widget-type': 'text',
        },
        {
          name: 'property17',
          label: 'Property 17',
          'widget-type': 'text',
        },
      ],
    },
  ],
  filters: [
    {
      name: 'Filter for property1 = true',
      condition: {
        expression: 'property1 == true',
      },
      show: [
        {
          name: 'property2',
          type: PropertyShowConfigTypeEnums.PROPERTY,
        },
        {
          name: 'property3',
          type: PropertyShowConfigTypeEnums.PROPERTY,
        },
      ],
    },
    {
      name: 'Filter for property1 = false',
      condition: {
        expression: 'property1 == false',
      },
      show: [
        {
          name: 'property4',
          type: PropertyShowConfigTypeEnums.PROPERTY,
        },
      ],
    },
    {
      name: 'Complex Filter - 1',
      condition: {
        expression: 'property5 > 2 && property5 < 100',
      },
      show: [
        {
          name: 'property7',
          type: PropertyShowConfigTypeEnums.PROPERTY,
        },
      ],
    },
    {
      name: 'Complex Filter - 2',
      condition: {
        expression: 'property6 == "VALUE1" || property6 == "VALUE2"',
      },
      show: [
        {
          name: 'property8',
          type: PropertyShowConfigTypeEnums.PROPERTY,
        },
      ],
    },
    {
      name: 'Complext Filter - 3',
      condition: {
        expression: 'property6 == "VALUE3" && property5 > 100',
      },
      show: [
        {
          name: 'property9',
          type: PropertyShowConfigTypeEnums.PROPERTY,
        },
      ],
    },
    {
      name: 'Hide entire group',
      condition: {
        expression: 'property1 != -1',
      },
      show: [
        {
          name: 'property5',
          type: PropertyShowConfigTypeEnums.PROPERTY,
        },
        {
          name: 'property6',
          type: PropertyShowConfigTypeEnums.PROPERTY,
        },
        {
          name: 'property7',
          type: PropertyShowConfigTypeEnums.PROPERTY,
        },
        {
          name: 'property8',
          type: PropertyShowConfigTypeEnums.PROPERTY,
        },
        {
          name: 'property9',
          type: PropertyShowConfigTypeEnums.PROPERTY,
        },
      ],
    },
    {
      name: 'Filter for simple condition objects - 1',
      condition: {
        property: 'property10',
        operator: CustomOperator.EQUALTO,
        value: 'property10',
      },
      show: [
        {
          name: 'property11',
        },
      ],
    },
    {
      name: 'Filter for simple condition objects - 2',
      condition: {
        property: 'property12',
        operator: CustomOperator.EXISTS,
      },
      show: [
        {
          name: 'property13',
        },
      ],
    },
    {
      name: 'Filter for simple condition objects - 3',
      condition: {
        property: 'property14',
        operator: CustomOperator.EQUALTO,
        value: 10,
      },
      show: [
        {
          name: 'property15',
        },
      ],
    },
    {
      name: 'Filter to hide entirely a group',
      condition: {
        expression: 'property1 == "Infinity"',
      },
      show: [
        {
          name: 'Hide entire group',
          type: PropertyShowConfigTypeEnums.GROUP,
        },
      ],
    },
  ],
};

function getPropertieObj(configGroups): Record<string, IFilteredWidgetProperty> {
  const propertiesObj = {};
  configGroups.forEach((group) =>
    group.properties.forEach((property) => {
      propertiesObj[property.name] = property;
    })
  );
  return propertiesObj;
}

function getFilteredConfigurationGroups() {
  const { defaultValues, configurationGroups } = processConfigurationGroups(
    pluginProperties,
    widgetJson['configuration-groups'],
    outputProperty
  );
  const filteredConfigurationGroups = filterByCondition(
    configurationGroups,
    widgetJson,
    pluginProperties,
    defaultValues
  );
  return { defaultValues, filteredConfigurationGroups, configurationGroups };
}

describe('Unit tests for Dynamic Plugin Filters', () => {
  it('Should correctly filter properties', () => {
    const {
      defaultValues,
      filteredConfigurationGroups,
      configurationGroups,
    } = getFilteredConfigurationGroups();
    let properties = getPropertieObj(filteredConfigurationGroups);
    let property2 = properties.property2;
    let property4 = properties.property4;
    let property3 = properties.property3;
    expect(property4.show).toBe(false);
    expect(property2.show && property3.show).toBe(true);
    expect(filteredConfigurationGroups);

    defaultValues.property1 = 'false';
    const modifiedFilteredConfigurationGroups = filterByCondition(
      configurationGroups,
      widgetJson,
      pluginProperties,
      defaultValues
    );
    properties = getPropertieObj(modifiedFilteredConfigurationGroups);
    property2 = properties.property2;
    property4 = properties.property4;
    property3 = properties.property3;
    expect(property4.show).toBe(true);
    expect(property2.show).toBe(false);
    expect(property3.show).toBe(false);
  });

  describe('Test complex condition: Complex Filter - 1', () => {
    const filteredConfigObj = getFilteredConfigurationGroups();
    const defaultValues = filteredConfigObj.defaultValues;
    let filteredConfigurationGroups = filteredConfigObj.filteredConfigurationGroups;
    const configurationGroups = filteredConfigObj.configurationGroups;

    /* 'property5 > 2 && property5 < 100' -> property7 */
    it('Should parse right for "value < 2"', () => {
      const properties = getPropertieObj(filteredConfigurationGroups);
      const property7 = properties.property7;
      expect(property7.show).toBe(false);
    });

    it('Should parse right for "value >2 && value < 100"', () => {
      defaultValues.property5 = '3';
      filteredConfigurationGroups = filterByCondition(
        configurationGroups,
        widgetJson,
        pluginProperties,
        defaultValues
      );
      const properties = getPropertieObj(filteredConfigurationGroups);
      const property7 = properties.property7;
      expect(property7.show).toBe(true);
    });

    it('Should parse right for "value > 100"', () => {
      defaultValues.property5 = '300';
      filteredConfigurationGroups = filterByCondition(
        configurationGroups,
        widgetJson,
        pluginProperties,
        defaultValues
      );
      const properties = getPropertieObj(filteredConfigurationGroups);
      const property7 = properties.property7;
      expect(property7.show).toBe(false);
    });
  });

  describe('Test complex condition: Complex Filter - 2', () => {
    const filteredConfigObj = getFilteredConfigurationGroups();
    const defaultValues = filteredConfigObj.defaultValues;
    let filteredConfigurationGroups = filteredConfigObj.filteredConfigurationGroups;
    const configurationGroups = filteredConfigObj.configurationGroups;

    /* 'property6 == "VALUE1" || property6 == "VALUE2"' */
    it('Should parse right for "property6 == "VALUE1" || property6 == "VALUE2"', () => {
      const properties = getPropertieObj(filteredConfigurationGroups);
      const property8 = properties.property8;
      expect(property8.show).toBe(true);
    });

    it('Should parse right for !(property6 == "VALUE1" || property6 == "VALUE2")', () => {
      defaultValues.property6 = 'VALUE3';
      filteredConfigurationGroups = filterByCondition(
        configurationGroups,
        widgetJson,
        pluginProperties,
        defaultValues
      );
      const properties = getPropertieObj(filteredConfigurationGroups);
      const property8 = properties.property8;
      expect(property8.show).toBe(false);
    });
  });

  describe('Test complex condition: Complex Filter - 3', () => {
    const filteredConfigObj = getFilteredConfigurationGroups();
    const defaultValues = filteredConfigObj.defaultValues;
    let filteredConfigurationGroups = filteredConfigObj.filteredConfigurationGroups;
    const configurationGroups = filteredConfigObj.configurationGroups;

    /* 'property6 == "VALUE3" && property5 > 100' */
    it('Should parse right for "property6 == "VALUE3" && property5 > 100"', () => {
      defaultValues.property6 = 'VALUE3';
      defaultValues.property5 = '200';
      filteredConfigurationGroups = filterByCondition(
        configurationGroups,
        widgetJson,
        pluginProperties,
        defaultValues
      );
      const properties = getPropertieObj(filteredConfigurationGroups);
      const property9 = properties.property9;
      expect(property9.show).toBe(true);
    });

    it('Should parse right for !(property6 == "VALUE3" && property5 > 100)', () => {
      defaultValues.property6 = 'VALUE1';
      filteredConfigurationGroups = filterByCondition(
        configurationGroups,
        widgetJson,
        pluginProperties,
        defaultValues
      );
      const properties = getPropertieObj(filteredConfigurationGroups);
      const property9 = properties.property9;
      expect(property9.show).toBe(false);
    });

    it('Should parse other properties right for "property6 == "VALUE3" && property5 > 100"', () => {
      const properties = getPropertieObj(filteredConfigurationGroups);
      const property7 = properties.property7;
      const property8 = properties.property8;
      expect(property7.show).toBe(false);
      // Because in previous test we set property6 to be 'VALUE1' which should make property8 to show up
      expect(property8.show).toBe(true);
    });
  });

  describe('Test hiding group', () => {
    it('Test that will hide entire group if all properties are hidden', () => {
      const filteredConfigGroupsObj = getFilteredConfigurationGroups();
      const defaultValues = filteredConfigGroupsObj.defaultValues;
      let filteredConfigurationGroups = filteredConfigGroupsObj.filteredConfigurationGroups;
      const configurationGroups = filteredConfigGroupsObj.configurationGroups;

      let groupWithComplexProperties = filteredConfigurationGroups.find(
        (group) => group.label === 'Group with complex filters'
      );
      expect(groupWithComplexProperties.show).toBe(true);
      defaultValues.property1 = '-1';
      filteredConfigurationGroups = filterByCondition(
        configurationGroups,
        widgetJson,
        pluginProperties,
        defaultValues
      );
      groupWithComplexProperties = filteredConfigurationGroups.find(
        (group) => group.label === 'Group with complex filters'
      );
      expect(groupWithComplexProperties.show).toBe(false);
    });

    it('Test that will hide entire group when the show has group type', () => {
      const filteredConfigGroupsObj = getFilteredConfigurationGroups();
      const defaultValues = filteredConfigGroupsObj.defaultValues;
      let filteredConfigurationGroups = filteredConfigGroupsObj.filteredConfigurationGroups;
      const configurationGroups = filteredConfigGroupsObj.configurationGroups;

      let groupWithComplexProperties = filteredConfigurationGroups.find(
        (group) => group.label === 'Hide entire group'
      );
      expect(groupWithComplexProperties.show).toBe(false);
      defaultValues.property1 = 'Infinity';
      filteredConfigurationGroups = filterByCondition(
        configurationGroups,
        widgetJson,
        pluginProperties,
        defaultValues
      );

      groupWithComplexProperties = filteredConfigurationGroups.find(
        (group) => group.label === 'Hide entire group'
      );
      expect(groupWithComplexProperties.show).toBe(true);
    });
  });

  describe('Test simple condition object - 1', () => {
    const filteredConfigObj = getFilteredConfigurationGroups();
    const defaultValues = filteredConfigObj.defaultValues;
    let filteredConfigurationGroups = filteredConfigObj.filteredConfigurationGroups;
    const configurationGroups = filteredConfigObj.configurationGroups;
    it('Should correctly show/hide properties satisfying basic condition - EQUAL TO', () => {
      const properties = getPropertieObj(filteredConfigurationGroups);
      const property11 = properties.property11;
      expect(property11.show).toBe(true);

      defaultValues.property10 = 'newproperty10';
      filteredConfigurationGroups = filterByCondition(
        configurationGroups,
        widgetJson,
        pluginProperties,
        defaultValues
      );
      const newProperties = getPropertieObj(filteredConfigurationGroups);
      const newproperty11 = newProperties.property11;
      expect(newproperty11.show).toBe(false);
    });

    it('Should correctly show/hide properties satisfying basic condition - EQUAL TO', () => {
      const properties = getPropertieObj(filteredConfigurationGroups);
      const property15 = properties.property15;
      expect(property15.show).toBe(true);

      defaultValues.property14 = 'property14';
      filteredConfigurationGroups = filterByCondition(
        configurationGroups,
        widgetJson,
        pluginProperties,
        defaultValues
      );
      const newProperties = getPropertieObj(filteredConfigurationGroups);
      const newproperty15 = newProperties.property15;
      expect(newproperty15.show).toBe(false);
    });

    it('Should correctly show/hide properties satisfying basic condition - EXISTS', () => {
      const properties = getPropertieObj(filteredConfigurationGroups);
      const property13 = properties.property13;
      expect(property13.show).toBe(false);

      defaultValues.property12 = 'property12';
      filteredConfigurationGroups = filterByCondition(
        configurationGroups,
        widgetJson,
        pluginProperties,
        defaultValues
      );
      const newProperties = getPropertieObj(filteredConfigurationGroups);
      const newproperty13 = newProperties.property13;
      expect(newproperty13.show).toBe(true);
    });
  });
});
