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

import { List, Map } from 'immutable';
import { getJSONOutput, parsePluginJSON } from 'components/PluginJSONCreator/utilities';
/**
 * We no longer automock all the modules. So this forces us to mock only those
 * modules we need to mock to run unit tests.
 *
 * Ideally we shouldn't test a module that includes DOM, but the given the nature of
 * configuration-groups and its dependency on material-ui I am adding specific mocks
 * for test.
 */
jest.mock('@material-ui/core/Box', () => {
  return {};
});
jest.mock('../../DataPrepConnections/PluginConnectionBrowser', () => {
  return {};
});
jest.mock('../../AbstractWidget/AbstractWidgetFactory', () => {
  return {
    WIDGET_FACTORY: {},
  };
});
const mockStateProperties = {
  displayName: 'test',
  configurationGroups: List([
    'ConfigGroup_a65136b3-9eef-4acd-b3af-ba315b314da6',
    'ConfigGroup_749496f0-ebd9-4334-a348-1a53ff7d1250',
  ]),
  groupToInfo: Map({
    'ConfigGroup_a65136b3-9eef-4acd-b3af-ba315b314da6': Map({
      label: 'Required',
    }),
    'ConfigGroup_749496f0-ebd9-4334-a348-1a53ff7d1250': Map({
      label: 'Non Required',
    }),
  }),
  groupToWidgets: Map({
    'ConfigGroup_a65136b3-9eef-4acd-b3af-ba315b314da6': List([
      'Widget_6a652bd3-d0e3-4177-b50f-f2af44af8aaa',
    ]),
    'ConfigGroup_749496f0-ebd9-4334-a348-1a53ff7d1250': List([
      'Widget_10a04a46-2b1c-4357-b350-4402e4a72287',
      'Widget_1fed5e89-338e-4416-a211-e85d7244b95a',
    ]),
  }),
  widgetInfo: Map({
    'Widget_6a652bd3-d0e3-4177-b50f-f2af44af8aaa': Map({
      widgetType: 'textbox',
      label: 'Required Property',
      name: 'requiredProperty',
    }),
    'Widget_10a04a46-2b1c-4357-b350-4402e4a72287': Map({
      widgetType: 'csv',
      label: 'Non Required Property',
      name: 'nonRequiredProperty',
    }),
    'Widget_1fed5e89-338e-4416-a211-e85d7244b95a': Map({
      widgetType: 'textarea',
      label: 'Non existing property',
      name: 'nonExistingProperty',
    }),
  }),
  widgetToAttributes: Map({
    'Widget_6a652bd3-d0e3-4177-b50f-f2af44af8aaa': Map({
      default: 'required property default value',
    }),
    'Widget_10a04a46-2b1c-4357-b350-4402e4a72287': Map({
      default: 'non required property default value',
    }),
    'Widget_1fed5e89-338e-4416-a211-e85d7244b95a': Map({
      default: 'this default value should not exist',
    }),
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
  filterToName: Map({
    'Filter_e8545fce-da5e-4551-945b-0ba8ab1eae17': 'Hide Properties',
  }),
  filterToCondition: Map({
    'Filter_e8545fce-da5e-4551-945b-0ba8ab1eae17': Map({
      property: 'requiredProperty',
      operator: 'exists',
    }),
  }),
  filterToShowlist: Map({
    'Filter_e8545fce-da5e-4551-945b-0ba8ab1eae17': List([
      'Show_f47eaa52-c09a-4a1f-b3fa-c0568e1298ff',
      'Show_b0aef571-e0ac-4055-82ff-a068c3f7c8b7',
    ]),
  }),
  showToInfo: Map({
    'Show_f47eaa52-c09a-4a1f-b3fa-c0568e1298ff': Map({
      name: 'nonRequiredProperty',
      type: 'property',
    }),
    'Show_b0aef571-e0ac-4055-82ff-a068c3f7c8b7': Map({
      name: 'nonExistingProperty',
      type: 'property',
    }),
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
      displayName,
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
    expect(pluginJSONData['display-name']).toEqual(displayName);

    // Check whether it correctly converted configuration groups state
    const configurationGroupsData = pluginJSONData['configuration-groups'];
    expect(configurationGroupsData.size).toEqual(configurationGroups.size);
    configurationGroupsData.forEach((groupData, groupIndex) => {
      const groupID = configurationGroups.get(groupIndex);
      expect(groupData.get('label')).toEqual(groupToInfo.get(groupID).get('label'));

      const widgetsData = configurationGroupsData.get(groupIndex).get('properties');
      expect(widgetsData.size).toEqual(groupToWidgets.get(groupID).size);
      widgetsData.forEach((widgetData, widgetIndex) => {
        const widgetID = groupToWidgets.get(groupID).get(widgetIndex);
        expect(widgetData.get('name')).toEqual(widgetInfo.get(widgetID).get('name'));
        expect(widgetData.get('label')).toEqual(widgetInfo.get(widgetID).get('label'));
        expect(widgetData.get('widget-type')).toEqual(widgetInfo.get(widgetID).get('widgetType'));
        expect(widgetData.get('widget-category')).toEqual(
          widgetInfo.get(widgetID).get('widgetCategory')
        );
        expect(widgetData.get('widget-attributes')).toMatchObject(widgetToAttributes.get(widgetID));
      });
    });

    // Check whether it correctly converted output state
    const outputData = pluginJSONData.outputs[0];
    expect(outputData.name).toEqual(outputName);
    expect(outputData['widget-type']).toEqual(outputWidgetType);
    expect(outputData['widget-attributes']['schema-types']).toEqual(schemaTypes);
    expect(outputData['widget-attributes']['schema-default-type']).toEqual(schemaDefaultType);

    // Check whether it correctly converted filter state
    const filterData = pluginJSONData.filters;
    filterData.forEach((filter, filterIndex) => {
      const filterID = filters.get(filterIndex);

      expect(filter.get('name')).toEqual(filterToName.get(filterID));
      expect(filter.get('condition')).toEqual(filterToCondition.get(filterID));
      expect(filter.get('show').size).toEqual(filterToShowlist.get(filterID).size);
      filter.get('show').forEach((showData, showIndex) => {
        const showID = filterToShowlist.get(filterID).get(showIndex);
        expect(showData).toMatchObject(showToInfo.get(showID));
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
    expect(pluginName).toEqual(mockPluginName);
    expect(pluginType).toEqual(mockPluginType);

    // Check whether it correctly parsed content for PluginInfoPage
    if (mockPluginJSONData['display-name']) {
      expect(displayName).toEqual(mockPluginJSONData['display-name']);
    }
    if (mockPluginJSONData['emit-alerts']) {
      expect(emitAlerts).toEqual(mockPluginJSONData['emit-alerts']);
    }
    if (mockPluginJSONData['emit-errors']) {
      expect(emitErrors).toEqual(mockPluginJSONData['emit-errors']);
    }

    // Check whether it correctly parsed content for ConfigurationGroupPage
    const configurationGroupsData = mockPluginJSONData['configuration-groups'];
    expect(configurationGroups.size).toEqual(configurationGroupsData.length);
    configurationGroups.forEach((groupID, groupIndex) => {
      const configurationGroupData = configurationGroupsData[groupIndex];
      const groupInfoState = groupToInfo.get(groupID);
      expect(groupInfoState.get('label')).toEqual(configurationGroupData.label);

      // Check whether widget-related data are correctly converted
      const widgets = groupToWidgets.get(groupID);
      expect(widgets.size).toEqual(configurationGroupData.properties.length);
      widgets.forEach((widgetID, widgetIndex) => {
        const widgetData = configurationGroupData.properties[widgetIndex];

        const widgetInfoState = widgetInfo.get(widgetID);
        expect(widgetInfoState.get('name')).toEqual(widgetData.name);
        expect(widgetInfoState.get('label')).toEqual(widgetData.label);
        expect(widgetInfoState.get('widgetType')).toEqual(widgetData['widget-type']);
        expect(widgetInfoState.get('widgetCategory')).toEqual(widgetData['widget-category']);

        const widgetAttributesState = widgetToAttributes.get(widgetID);
        List(widgetAttributesState.keySeq()).forEach((fieldName: string) => {
          const widgetAttributeValuesState = widgetAttributesState.get(fieldName);
          expect(widgetAttributeValuesState).toEqual(
            (widgetData['widget-attributes'] as any)[fieldName]
          );
        });
      });
    });

    // Check whether it correctly parsed content for OutputPage
    const outputData = mockPluginJSONData.outputs[0];
    expect(outputWidgetType).toEqual(outputData['widget-type']);
    expect(outputName).toEqual(outputData.name);
    expect(schemaTypes).toEqual(outputData['widget-attributes']['schema-types']);
    expect(schemaDefaultType).toEqual(outputData['widget-attributes']['schema-default-type']);

    // Check whether it correctly parsed content for FilterPage
    const filtersData = mockPluginJSONData.filters;
    expect(filters.size).toEqual(filtersData.length);
    filters.forEach((filterID, filterIndex) => {
      const filterData = filtersData[filterIndex];

      const filterNameState = filterToName.get(filterID);
      expect(filterNameState).toEqual(filterData.name);

      const filterConditionState = filterToCondition.get(filterID);
      expect(filterConditionState.toJS()).toMatchObject(filterData.condition);

      // Check whether showlist data are correctly converted
      const shows = filterToShowlist.get(filterID);
      expect(shows.size).toEqual(filterData.show.length);
      shows.forEach((showID, showIndex) => {
        const showData = filterData.show[showIndex];

        const showInfoState = showToInfo.get(showID);
        expect(showInfoState.get('name')).toEqual(showData.name);
        expect(showInfoState.get('type')).toEqual(showData.type);
      });
    });
  });
});
