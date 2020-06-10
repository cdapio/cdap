import { IBasicPluginInfo } from 'components/PluginJSONCreator/CreateContextConnect';
import { fromJS, List, Map } from 'immutable';
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
    const groupLabel = groupToInfo.get(groupID).get('label');
    const widgetData = groupToWidgets.get(groupID).map((widgetID: string) => {
      const info = widgetInfo.get(widgetID);
      const widgetAttributes = widgetToAttributes.get(widgetID);

      return fromJS({
        'widget-type': info.get('widgetType'),
        label: info.get('label'),
        name: info.get('name'),
        ...(info.get('widgetCategory') && {
          'widget-category': info.get('widgetCategory'),
        }),
        ...(widgetAttributes &&
          List(widgetAttributes.keys()).size > 0 && {
            'widget-attributes': widgetAttributes,
          }),
      });
    });
    return fromJS({
      label: groupLabel,
      properties: widgetData,
    });
  });

  const outputsData = {
    ...(outputName && { name: outputName }),
  };

  const filtersData = filters.map((filterID) => {
    const filterToShowListData = filterToShowList.get(filterID).map((showID) => {
      return fromJS({
        name: showToInfo.get(showID).get('name'),
        ...(showToInfo.get(showID).get('type') && {
          type: showToInfo.get(showID).get('type'),
        }),
      });
    });
    return fromJS({
      name: filterToName.get(filterID),
      condition: filterToCondition.get(filterID),
      show: filterToShowListData,
    });
  });

  const config = {
    metadata: {
      'spec-version': '1.5',
    },
    ...(displayName && { 'display-name': displayName }),
    ...(emitAlerts && { 'emit-alerts': emitAlerts }),
    ...(emitErrors && { 'emit-errors': emitErrors }),
    'configuration-groups': configurationGroupsData || List(),
    ...(outputsData &&
      Object.keys(outputsData).length > 0 && {
        outputs: [outputsData],
      }),
    ...(filtersData &&
      filtersData.size > 0 && {
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

  let newConfigurationGroups = List([]);
  let newGroupToInfo = Map({});
  let newGroupToWidgets = Map({});
  let newWidgetInfo = Map({});
  let newWidgetToAttributes = Map({});
  let newFilters = List([]);
  let newFilterToName = Map({});
  let newFilterToCondition = Map({});
  let newFilterToShowList = Map({});
  let newShowToInfo = Map({});

  pluginJSON['configuration-groups'].forEach((groupObj) => {
    if (!groupObj || Object.keys(groupObj).length === 0) {
      return;
    }
    const groupLabel = groupObj.label;

    // generate a unique group ID
    const newGroupID = 'ConfigGroup_' + uuidV4();

    newConfigurationGroups = newConfigurationGroups.push(newGroupID);

    newGroupToInfo = newGroupToInfo.set(
      newGroupID,
      fromJS({
        label: groupLabel,
      })
    );

    newGroupToWidgets = newGroupToWidgets.set(newGroupID, fromJS([]));

    const groupWidgets = groupObj.properties;
    groupWidgets.forEach((widgetObj) => {
      // generate a unique widget ID
      const newWidgetID = 'Widget_' + uuidV4();

      newGroupToWidgets = newGroupToWidgets.update(newGroupID, (widgets) =>
        fromJS(widgets).push(newWidgetID)
      );

      newWidgetInfo = newWidgetInfo.set(
        newWidgetID,
        fromJS({
          widgetType: widgetObj['widget-type'],
          label: widgetObj.label,
          name: widgetObj.name,
          ...(widgetObj['widget-category'] && { widgetCategory: widgetObj['widget-category'] }),
        })
      );

      if (
        widgetObj['widget-attributes'] &&
        Object.keys(widgetObj['widget-attributes']).length > 0
      ) {
        newWidgetToAttributes = newWidgetToAttributes.set(
          newWidgetID,
          fromJS(widgetObj['widget-attributes'])
        );
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

      newFilters = newFilters.push(newFilterID);

      newFilterToName = newFilterToName.set(newFilterID, filterObj.name);
      newFilterToCondition = newFilterToCondition.set(newFilterID, fromJS(filterObj.condition));

      newFilterToShowList = newFilterToShowList.set(newFilterID, fromJS([]));

      if (filterObj.show) {
        filterObj.show.map((showObj) => {
          const newShowID = 'Show_' + uuidV4();

          newFilterToShowList = newFilterToShowList.update(newFilterID, (showlist) =>
            fromJS(showlist).push(newShowID)
          );

          newShowToInfo = newShowToInfo.set(
            newShowID,
            fromJS({
              name: showObj.name,
              ...(showObj.type && { type: showObj.type }),
            })
          );
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
