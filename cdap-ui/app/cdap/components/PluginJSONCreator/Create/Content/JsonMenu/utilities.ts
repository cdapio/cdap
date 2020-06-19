import { IPropertyShowConfig } from 'components/ConfigurationGroup/types';
import { SPEC_VERSION } from 'components/PluginJSONCreator/constants';
import {
  IBasicPluginInfo,
  IConfigurationGroupInfo,
  IWidgetInfo,
} from 'components/PluginJSONCreator/CreateContextConnect';
import fileDownload from 'js-file-download';
import uuidV4 from 'uuid/v4';

function getJSONConfig(widgetJSONData) {
  const {
    displayName,
    emitAlerts,
    emitErrors,
    configurationGroups,
    groupToInfo,
    groupToWidgets,
    widgetInfo,
    widgetToAttributes,
    filters,
    filterToName,
    filterToCondition,
    filterToShowList,
    showToInfo,
    outputName,
  } = widgetJSONData;

  const configurationGroupsData = configurationGroups.map((groupID: string) => {
    const groupLabel = groupToInfo[groupID].label;
    const widgetData = groupToWidgets[groupID].map((widgetID: string) => {
      const info: IWidgetInfo = widgetInfo[widgetID];
      const widgetAttributes = widgetToAttributes[widgetID];

      return {
        'widget-type': info.widgetType,
        label: info.label,
        name: info.name,
        ...(info.widgetCategory && { 'widget-category': info.widgetCategory }),
        ...(widgetAttributes &&
          Object.keys(widgetAttributes).length > 0 && {
            'widget-attributes': widgetAttributes,
          }),
      };
    });
    return {
      label: groupLabel,
      properties: widgetData,
    };
  });

  const filtersData = filters.map((filterID) => {
    const filterToShowListData = filterToShowList[filterID].map((showID) => {
      let showListData;
      if (showToInfo[showID].type) {
        showListData = {
          name: showToInfo[showID].name,
          type: showToInfo[showID].type,
        };
      } else {
        showListData = {
          name: showToInfo[showID].name,
        };
      }
      return showListData;
    });
    return {
      name: filterToName[filterID],
      condition: filterToCondition[filterID],
      show: filterToShowListData,
    };
  });

  const config = {
    metadata: {
      'spec-version': SPEC_VERSION,
    },
    ...(displayName && { 'display-name': displayName }),
    ...(emitAlerts && { 'emit-alerts': emitAlerts }),
    ...(emitErrors && { 'emit-errors': emitErrors }),
    'configuration-groups': configurationGroupsData,
    outputs: [
      {
        name: outputName,
      },
    ],
    ...(filtersData &&
      Object.keys(filtersData).length > 0 && {
        filters: filtersData,
      }),
  };

  return config;
}

function parsePluginJSON(filename, pluginJSON) {
  // Parse filename in order to set pluginName and pluginType
  // Currently the filename is designed to be <pluginName>-<pluginType>.json
  const [pluginName, pluginType] = filename.split('-');

  // Parse file data in order to populate the rest of properties
  const basicPluginInfo = {
    // If the string fields are undefined, set them to empty string
    displayName: pluginJSON['display-name'] ? pluginJSON['display-name'] : '',
    pluginName: pluginName ? pluginName : '',
    pluginType: pluginType ? pluginType : '',
    emitAlerts: pluginJSON['emit-alerts'],
    emitErrors: pluginJSON['emit-errors'],
  } as IBasicPluginInfo;

  const newConfigurationGroups = [];
  const newGroupToInfo = {};
  const newGroupToWidgets = {};
  const newWidgetInfo = {};
  const newWidgetToAttributes = {};
  const newFilters: string[] = [];
  const newFilterToName = {};
  const newFilterToCondition = {};
  const newFilterToShowList = {};
  const newShowToInfo = {};

  pluginJSON['configuration-groups'].forEach((groupObj) => {
    if (!groupObj || Object.keys(groupObj).length === 0) {
      return;
    }
    const groupLabel = groupObj.label;

    // generate a unique group ID
    const newGroupID = 'ConfigGroup_' + uuidV4();

    newConfigurationGroups.push(newGroupID);

    newGroupToInfo[newGroupID] = {
      label: groupLabel,
    } as IConfigurationGroupInfo;

    newGroupToWidgets[newGroupID] = [];

    const groupWidgets = groupObj.properties;
    groupWidgets.forEach((widgetObj) => {
      // generate a unique widget ID
      const newWidgetID = 'Widget_' + uuidV4();

      newGroupToWidgets[newGroupID].push(newWidgetID);

      const info = {
        widgetType: widgetObj['widget-type'],
        label: widgetObj.label,
        name: widgetObj.name,
        ...(widgetObj['widget-category'] && { widgetCategory: widgetObj['widget-category'] }),
      } as IWidgetInfo;

      newWidgetInfo[newWidgetID] = info;

      if (
        widgetObj['widget-attributes'] &&
        Object.keys(widgetObj['widget-attributes']).length > 0
      ) {
        newWidgetToAttributes[newWidgetID] = widgetObj['widget-attributes'];
      }
    });
  });

  const newOutputName =
    pluginJSON.outputs && pluginJSON.outputs.length > 0 ? pluginJSON.outputs[0].name : '';

  if (pluginJSON.filters) {
    pluginJSON.filters.forEach((filterObj) => {
      if (!filterObj || Object.keys(filterObj).length === 0) {
        return;
      }

      // generate a unique filter ID
      const newFilterID = 'Filter_' + uuidV4();

      newFilters.push(newFilterID);

      newFilterToName[newFilterID] = filterObj.name;
      newFilterToCondition[newFilterID] = filterObj.condition;

      newFilterToShowList[newFilterID] = [];

      if (filterObj.show) {
        filterObj.show.map((showObj) => {
          const newShowID = 'Show_' + uuidV4();

          newFilterToShowList[newFilterID].push(newShowID);

          newShowToInfo[newShowID] = {
            name: showObj.name,
            ...(showObj.type && { type: showObj.type }),
          } as IPropertyShowConfig;
        });
      }
    });
  }

  return {
    basicPluginInfo,
    newConfigurationGroups,
    newGroupToInfo,
    newGroupToWidgets,
    newWidgetInfo,
    newWidgetToAttributes,
    newOutputName,
    newFilters,
    newFilterToName,
    newFilterToCondition,
    newFilterToShowList,
    newShowToInfo,
  };
}

function downloadPluginJSON(widgetJSONData) {
  const JSONConfig = getJSONConfig(widgetJSONData);
  const { pluginName, pluginType } = widgetJSONData;
  fileDownload(JSON.stringify(JSONConfig, undefined, 4), `${pluginName}-${pluginType}.json`);
}

export { getJSONConfig, parsePluginJSON, downloadPluginJSON };
