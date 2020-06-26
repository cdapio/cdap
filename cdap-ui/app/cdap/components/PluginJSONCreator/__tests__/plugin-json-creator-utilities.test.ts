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

import { getJSONOutput, parsePluginJSON } from 'components/PluginJSONCreator/utilities';
import { List, Map } from 'immutable';

const mockStateProperties = {
  pluginName: 'test',
  pluginType: 'batchsource',
  configurationGroups: List([
    'ConfigGroup_a65136b3-9eef-4acd-b3af-ba315b314da6',
    'ConfigGroup_749496f0-ebd9-4334-a348-1a53ff7d1250',
  ]),
  groupToInfo: Map<string, Map<string, string>>({
    'ConfigGroup_a65136b3-9eef-4acd-b3af-ba315b314da6': {
      label: 'Required',
    },
    'ConfigGroup_749496f0-ebd9-4334-a348-1a53ff7d1250': {
      label: 'Non Required',
    },
  }),
  groupToWidgets: Map<string, List<string>>({
    'ConfigGroup_a65136b3-9eef-4acd-b3af-ba315b314da6': [
      'Widget_6a652bd3-d0e3-4177-b50f-f2af44af8aaa',
    ],
    'ConfigGroup_749496f0-ebd9-4334-a348-1a53ff7d1250': [
      'Widget_10a04a46-2b1c-4357-b350-4402e4a72287',
      'Widget_1fed5e89-338e-4416-a211-e85d7244b95a',
    ],
  }),
  widgetInfo: Map<string, Map<string, string>>({
    'Widget_6a652bd3-d0e3-4177-b50f-f2af44af8aaa': {
      widgetType: 'textbox',
      label: 'Required Property',
      name: 'requiredProperty',
    },
    'Widget_10a04a46-2b1c-4357-b350-4402e4a72287': {
      widgetType: 'csv',
      label: 'Non Required Property',
      name: 'nonRequiredProperty',
    },
    'Widget_1fed5e89-338e-4416-a211-e85d7244b95a': {
      widgetType: 'textarea',
      label: 'Non existing property',
      name: 'nonExistingProperty',
    },
  }),
  widgetToAttributes: Map<string, Map<string, any>>({
    'Widget_6a652bd3-d0e3-4177-b50f-f2af44af8aaa': {
      default: 'required property default value',
    },
    'Widget_10a04a46-2b1c-4357-b350-4402e4a72287': {
      default: 'non required property default value',
    },
    'Widget_1fed5e89-338e-4416-a211-e85d7244b95a': {
      default: 'this default value should not exist',
    },
  }),
  outputName: 'schema',
  outputWidgetType: 'schema',
  schemaTypes: [
    'boolean',
    'int',
    'long',
    'float',
    'double',
    'bytes',
    'string',
    'array',
    'record',
    'map',
    'union',
  ],
  schemaDefaultType: 'string',
  filters: List(['Filter_e8545fce-da5e-4551-945b-0ba8ab1eae17']),
  filterToName: Map<string, string>({
    'Filter_e8545fce-da5e-4551-945b-0ba8ab1eae17': 'Hide Properties',
  }),
  filterToCondition: Map<string, Map<string, string>>({
    'Filter_e8545fce-da5e-4551-945b-0ba8ab1eae17': {
      property: 'requiredProperty',
      operator: 'exists',
    },
  }),
  filterToShowlist: Map<string, List<string>>({
    'Filter_e8545fce-da5e-4551-945b-0ba8ab1eae17': [
      'Show_f47eaa52-c09a-4a1f-b3fa-c0568e1298ff',
      'Show_b0aef571-e0ac-4055-82ff-a068c3f7c8b7',
    ],
  }),
  showToInfo: Map<string, Map<string, string>>({
    'Show_f47eaa52-c09a-4a1f-b3fa-c0568e1298ff': {
      name: 'nonRequiredProperty',
      type: 'property',
    },
    'Show_b0aef571-e0ac-4055-82ff-a068c3f7c8b7': {
      name: 'nonExistingProperty',
      type: 'property',
    },
  }),
};

const mockPluginJSONData = {
  metadata: {
    'spec-version': '1.5',
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
      name: 'schema',
      label: 'schema',
      'widget-type': 'schema',
      'widget-attributes': {
        'schema-types': [
          'boolean',
          'int',
          'long',
          'float',
          'double',
          'bytes',
          'string',
          'array',
          'record',
          'map',
          'union',
        ],
        'schema-default-type': 'string',
        'property-watch': 'format',
      },
    },
  ],
  filters: [
    {
      name: 'Hide Properties',
      condition: {
        property: 'requiredProperty',
        operator: 'exists',
      },
      show: [
        {
          name: 'nonRequiredProperty',
          type: 'property',
        },
        {
          name: 'nonExistingProperty',
          type: 'property',
        },
      ],
    },
  ],
};

describe('Plugin JSON Parser', () => {
  it('should convert state properties into plugin JSON', () => {
    const pluginJSONData = getJSONOutput(mockStateProperties);

    const {
      pluginName,
      pluginType,
      configurationGroups,
      groupToInfo,
      groupToWidgets,
      widgetInfo,
      widgetToAttributes,
      outputName,
      outputWidgetType,
      schemaTypes,
      schemaDefaultType,
      filters,
      filterToName,
      filterToCondition,
      filterToShowlist,
      showToInfo,
    } = mockStateProperties;

    // Check whether it correctly converted plugin infromation state
    expect(pluginJSONData['plugin-name']).toBe(pluginName);
    expect(pluginJSONData['plugin-type']).toBe(pluginType);

    // Check whether it correctly converted configuration groups state
    const configurationGroupsData = pluginJSONData['configuration-groups'];
    expect(configurationGroupsData.length).toBe(configurationGroups.size);
    configurationGroupsData.forEach((groupData, groupIndex) => {
      const groupID = configurationGroups.get(groupIndex);
      expect(groupData.label).toBe(groupToInfo.get(groupID).get('label'));
      expect(groupData.properties.length).toBe(groupToWidgets.get(groupID).size);

      const widgetsData = configurationGroupsData[groupIndex].properties;
      widgetsData.forEach((widgetData, widgetIndex) => {
        const widgetID = groupToWidgets.get(groupID).get(widgetIndex);
        expect(widgetData.name).toBe(widgetInfo.get(widgetID).get('name'));
        expect(widgetData.label).toBe(widgetInfo.get(widgetID).get('label'));
        expect(widgetData['widget-type']).toBe(widgetInfo.get(widgetID).get('widgetType'));
        expect(widgetData['widget-category']).toBe(widgetInfo.get(widgetID).get('widgetCategory'));
        expect(widgetData['widget-attributes']).toBe(widgetToAttributes.get(widgetID).toJS());
      });
    });

    // Check whether it correctly converted output state
    const outputData = pluginJSONData.outputs[0];
    expect(outputData.name).toBe(outputName);
    expect(outputData['widget-type']).toBe(outputWidgetType);
    expect(outputData['widget-attributes']['schema-types']).toBe(schemaTypes);
    expect(outputData['widget-attributes']['schema-default-type']).toBe(schemaDefaultType);

    // Check whether it correctly converted filter state
    const filterData = pluginJSONData.filters;
    filterData.forEach((filter, filterIndex) => {
      const filterID = filters.get(filterIndex);

      expect(filter.name).toBe(filterToName.get(filterID));
      expect(filter.condition).toBe(filterToCondition.get(filterID).toJS());
      expect(filter.show.length).toBe(filterToShowlist.get(filterID).size);
      filter.show.forEach((showData, showIndex) => {
        const showID = filterToShowlist.get(filterID).get(showIndex);
        expect(showData).toBe(showToInfo.get(showID).toJS());
      });
    });
  });

  it('should parse plugin JSON file into state properties', () => {
    const mockPluginName = 'test';
    const mockPluginType = 'batchsource';
    const stateProperties = parsePluginJSON(
      `${mockPluginName}-${mockPluginType}.json`,
      mockPluginJSONData
    );

    const {
      pluginName,
      pluginType,
      displayName,
      emitAlerts,
      emitErrors,
      configurationGroups,
      groupToInfo,
      groupToWidgets,
      widgetInfo,
      widgetToAttributes,
      outputName,
      outputWidgetType,
      schemaTypes,
      schemaDefaultType,
      filters,
      filterToName,
      filterToCondition,
      filterToShowlist,
      showToInfo,
    } = stateProperties;

    // Check whether it correctly parsed the filename
    expect(pluginName).toBe(mockPluginName);
    expect(pluginType).toBe(mockPluginType);

    // Check whether it correctly parsed content for PluginInfoPage
    expect(displayName).toBe(mockPluginJSONData['display-name']);
    expect(emitAlerts).toBe(mockPluginJSONData['emit-alerts']);
    expect(emitErrors).toBe(mockPluginJSONData['emit-errors']);

    // Check whether it correctly parsed content for ConfigurationGroupPage
    const configurationGroupsData = mockPluginJSONData['configuration-groups'];
    expect(configurationGroups.size).toBe(configurationGroupsData.length);
    configurationGroups.forEach((groupID, groupIndex) => {
      const configurationGroupData = configurationGroupsData[groupIndex];
      const groupInfoState = groupToInfo.get(groupID);
      expect(groupInfoState.get('label')).toBe(configurationGroupData.label);

      // Check whether widget-related data are correctly converted
      const widgets = groupToWidgets.get(groupID);
      expect(widgets.size).toBe(configurationGroupData.properties.length);
      widgets.forEach((widgetID, widgetIndex) => {
        const widgetData = configurationGroupData.properties[widgetIndex];

        const widgetInfoState = widgetInfo.get(widgetID);
        expect(widgetInfoState.get('name')).toBe(widgetData.name);
        expect(widgetInfoState.get('label')).toBe(widgetData.label);
        expect(widgetInfoState.get('widgetType')).toBe(widgetData['widget-type']);
        expect(widgetInfoState.get('widgetCategory')).toBe(widgetData['widget-category']);

        const widgetAttributesState = widgetToAttributes.get(widgetID);
        Object.keys(widgetAttributesState).forEach((field) => {
          const widgetAttributeValuesState = widgetAttributesState.get(field);
          expect(widgetAttributeValuesState.toJS()).toBe(widgetData['widget-attributes']);
        });
      });
    });

    // Check whether it correctly parsed content for OutputPage
    const outputData = mockPluginJSONData.outputs[0];
    expect(outputWidgetType).toBe(outputData['widget-type']);
    expect(outputName).toBe(outputData.name);
    expect(schemaTypes).toBe(outputData['widget-attributes']['schema-types']);
    expect(schemaDefaultType).toBe(outputData['widget-attributes']['schema-default-type']);

    // Check whether it correctly parsed content for FilterPage
    const filtersData = mockPluginJSONData.filters;
    expect(filters.size).toBe(filtersData.length);
    filters.forEach((filterID, filterIndex) => {
      const filterData = filtersData[filterIndex];

      const filterNameState = filterToName.get(filterID);
      expect(filterNameState).toBe(filterData.name);

      const filterConditionState = filterToCondition.get(filterID);
      expect(filterConditionState.toJS()).toBe(filterData.condition);

      // Check whether showlist data are correctly converted
      const shows = filterToShowlist.get(filterID);
      expect(shows.size).toBe(filterData.show.length);
      shows.forEach((showID, showIndex) => {
        const showData = filterData[showIndex];

        const showInfoState = showToInfo.get(showID);
        expect(showInfoState.get('name')).toBe(showData.name);
        expect(showInfoState.get('type')).toBe(showData.type);
      });
    });
  });
});
