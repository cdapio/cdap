import { List, Map, fromJS } from 'immutable';
import { SPEC_VERSION, SchemaType } from 'components/PluginJSONCreator/constants';

import fileDownload from 'js-file-download';
import uuidV4 from 'uuid/v4';

/**
 * Convert state properties into plugin JSON file data.
 *
 * @param widgetData State properties for plugin JSON
 */
export function getJSONOutput(widgetData) {
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
    filterToShowlist,
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

      return Map({
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
    return Map({
      label,
      properties,
    });
  });

  let outputData;
  if (outputWidgetType === SchemaType.Explicit) {
    outputData = {
      name: outputName || '',
      'widget-type': outputWidgetType,
      'widget-attributes': {
        'schema-default-type': schemaDefaultType || '',
        'schema-types': schemaTypes || [],
      },
    };
  } else {
    outputData = {
      'widget-type': outputWidgetType,
      schema: schema || {},
    };
  }

  const filtersData = filters.map((filterID) => {
    const filterToShowlistData = filterToShowlist.get(filterID).map((showID) => {
      return Map({
        name: showToInfo.get(showID).get('name'),
        ...(showToInfo.get(showID).get('type') && {
          type: showToInfo.get(showID).get('type'),
        }),
      });
    });
    return Map({
      name: filterToName.get(filterID),
      condition: filterToCondition.get(filterID),
      show: filterToShowlistData,
    });
  });

  const config = {
    metadata: {
      'spec-version': SPEC_VERSION,
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

/**
 * Parse the plugin JSON file into the state properties.
 *
 * @param filename Incoming filename
 * @param pluginJSON Incoming JSON file data
 */
export function parsePluginJSON(filename, pluginJSON) {
  const filenameWithoutExtension = filename.substring(0, filename.lastIndexOf('.')) || filename;

  // Parse filename in order to set pluginName and pluginType
  // Currently the filename is designed to be <pluginName>-<pluginType>.json
  const pluginName = filenameWithoutExtension.split('-')[0] || '';
  const pluginType = filenameWithoutExtension.split('-')[1] || '';

  // Parse file data in order to populate the rest of properties
  // If the string fields are undefined, set them to empty string
  const displayName = pluginJSON['display-name'] ? pluginJSON['display-name'] : '';
  const emitAlerts = pluginJSON['emit-alerts'];
  const emitErrors = pluginJSON['emit-errors'];

  let configurationGroups = List<string>([]);
  let groupToInfo = Map<string, Map<string, string>>({});
  let groupToWidgets = Map<string, List<string>>({});
  let widgetInfo = Map<string, Map<string, string>>({});
  let widgetToAttributes = Map<string, Map<string, any>>({});
  let filters = List<string>([]);
  let filterToName = Map<string, string>({});
  let filterToCondition = Map<string, Map<string, string>>({});
  let filterToShowlist = Map<string, List<string>>({});
  let showToInfo = Map<string, Map<string, string>>({});

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
        Map({
          label: groupLabel,
        })
      );

      groupToWidgets = groupToWidgets.set(newGroupID, List([]));

      const groupWidgets = groupObj.properties;
      if (groupWidgets) {
        groupWidgets.forEach((widgetObj) => {
          // generate a unique widget ID
          const newWidgetID = 'Widget_' + uuidV4();

          groupToWidgets = groupToWidgets.update(newGroupID, (widgets) =>
            widgets.push(newWidgetID)
          );

          widgetInfo = widgetInfo.set(
            newWidgetID,
            Map({
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
            // Widget attributes "options" and "values" are used interchangeably.
            // Thus, unify them into "options".
            if (widgetObj['widget-attributes'].values) {
              Object.defineProperty(
                widgetObj['widget-attributes'],
                'options',
                Object.getOwnPropertyDescriptor(widgetObj['widget-attributes'], 'values')
              );
              delete widgetObj['widget-attributes'].values;
            }

            widgetToAttributes = widgetToAttributes.set(
              newWidgetID,
              fromJS(widgetObj['widget-attributes'])
            );
          }
        });
      }
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
      filterToCondition = filterToCondition.set(newFilterID, Map(filterObj.condition));

      filterToShowlist = filterToShowlist.set(newFilterID, List([]));

      if (filterObj.show) {
        filterObj.show.map((showObj) => {
          const newShowID = 'Show_' + uuidV4();

          filterToShowlist = filterToShowlist.update(newFilterID, (showlist) =>
            showlist.push(newShowID)
          );

          showToInfo = showToInfo.set(
            newShowID,
            Map({
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
    filterToShowlist,
    showToInfo,
  };
}

/**
 * Download the live JSON output.
 *
 * @param widgetData State properties for plugin JSON
 */
export function downloadPluginJSON(widgetData) {
  const JSONOutput = getJSONOutput(widgetData);
  const { pluginName, pluginType } = widgetData;
  fileDownload(JSON.stringify(JSONOutput, undefined, 4), `${pluginName}-${pluginType}.json`);
}
