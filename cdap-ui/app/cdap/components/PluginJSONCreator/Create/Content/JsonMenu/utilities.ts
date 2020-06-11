import { SchemaType } from 'components/PluginJSONCreator/constants';
import { fromJS, List, Map } from 'immutable';
import fileDownload from 'js-file-download';
import uuidV4 from 'uuid/v4';

function getJSONOutput(widgetData) {
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
    outputWidgetType,
    schemaTypes,
    schemaDefaultType,
    schema,
  } = widgetData;

  const configurationGroupsData = configurationGroups.map((groupID: string) => {
    const label = groupToInfo.get(groupID).get('label');
    const properties = groupToWidgets.get(groupID).map((widgetID: string) => {
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
      label,
      properties,
    });
  });

  const outputData = {
    ...(outputName && { name: outputName }),
    'widget-type': outputWidgetType,
    ...(outputWidgetType === SchemaType.Implicit && { schema: schema || {} }),
    ...(outputWidgetType === SchemaType.Explicit && {
      'widget-attributes': {
        'schema-default-type': schemaDefaultType || '',
        'schema-types': schemaTypes || [],
      },
    }),
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
    ...(outputData &&
      Object.keys(outputData).length > 0 && {
        outputs: [outputData],
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
  const pluginName = filename.split('-')[0] || '';
  const pluginType = filename.split('-')[1] || '';

  // Parse file data in order to populate the rest of properties
  // If the string fields are undefined, set them to empty string
  const displayName = pluginJSON['display-name'] ? pluginJSON['display-name'] : '';
  const emitAlerts = pluginJSON['emit-alerts'];
  const emitErrors = pluginJSON['emit-errors'];

  let configurationGroups = List([]);
  let groupToInfo = Map({});
  let groupToWidgets = Map({});
  let widgetInfo = Map({});
  let widgetToAttributes = Map({});
  let filters = List([]);
  let filterToName = Map({});
  let filterToCondition = Map({});
  let filterToShowList = Map({});
  let showToInfo = Map({});

  if (pluginJSON['configuration-groups']) {
    pluginJSON['configuration-groups'].forEach((groupObj) => {
      if (!groupObj || Object.keys(groupObj).length === 0) {
        return;
      }
      const groupLabel = groupObj.label;

      // generate a unique group ID
      const newGroupID = 'ConfigGroup_' + uuidV4();

      configurationGroups = configurationGroups.push(newGroupID);

      groupToInfo = groupToInfo.set(
        newGroupID,
        fromJS({
          label: groupLabel,
        })
      );

      groupToWidgets = groupToWidgets.set(newGroupID, fromJS([]));

      const groupWidgets = groupObj.properties;
      groupWidgets.forEach((widgetObj) => {
        // generate a unique widget ID
        const newWidgetID = 'Widget_' + uuidV4();

        groupToWidgets = groupToWidgets.update(newGroupID, (widgets) =>
          fromJS(widgets).push(newWidgetID)
        );

        widgetInfo = widgetInfo.set(
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
          widgetToAttributes = widgetToAttributes.set(
            newWidgetID,
            fromJS(widgetObj['widget-attributes'])
          );
        }
      });
    });
  }

  let outputName;
  let outputWidgetType;
  let schemaTypes;
  let schemaDefaultType;
  let schema;
  if (pluginJSON.outputs && pluginJSON.outputs.length > 0) {
    const output = pluginJSON.outputs[0];
    outputName = output.name || '';
    outputWidgetType = output['widget-type'] || '';

    const schemaAttributes = output['widget-attributes'] || '';
    if (schemaAttributes) {
      schemaTypes = schemaAttributes['schema-types'] || [];
      schemaDefaultType = schemaAttributes['schema-default-type'] || '';
    }
    schema = output.schema || {};
  }

  if (pluginJSON.filters) {
    pluginJSON.filters.forEach((filterObj) => {
      if (!filterObj || Object.keys(filterObj).length === 0) {
        return;
      }

      // generate a unique filter ID
      const newFilterID = 'Filter_' + uuidV4();

      filters = filters.push(newFilterID);

      filterToName = filterToName.set(newFilterID, filterObj.name);
      filterToCondition = filterToCondition.set(newFilterID, fromJS(filterObj.condition));

      filterToShowList = filterToShowList.set(newFilterID, fromJS([]));

      if (filterObj.show) {
        filterObj.show.map((showObj) => {
          const newShowID = 'Show_' + uuidV4();

          filterToShowList = filterToShowList.update(newFilterID, (showlist) =>
            fromJS(showlist).push(newShowID)
          );

          showToInfo = showToInfo.set(
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
    schema,
    filters,
    filterToName,
    filterToCondition,
    filterToShowList,
    showToInfo,
  };
}

function downloadPluginJSON(widgetData) {
  const JSONOutput = getJSONOutput(widgetData);
  const { pluginName, pluginType } = widgetData;
  fileDownload(JSON.stringify(JSONOutput, undefined, 4), `${pluginName}-${pluginType}.json`);
}

export { getJSONOutput, parsePluginJSON, downloadPluginJSON };
