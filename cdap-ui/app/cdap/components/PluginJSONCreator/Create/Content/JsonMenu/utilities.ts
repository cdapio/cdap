import { SPEC_VERSION } from 'components/PluginJSONCreator/constants';
import { IWidgetInfo } from 'components/PluginJSONCreator/CreateContextConnect';
import fileDownload from 'js-file-download';

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

  const outputsData = {
    ...(outputName && { name: outputName }),
  };

  const config = {
    metadata: {
      'spec-version': SPEC_VERSION,
    },
    ...(displayName && { 'display-name': displayName }),
    ...(emitAlerts && { 'emit-alerts': emitAlerts }),
    ...(emitErrors && { 'emit-errors': emitErrors }),
    'configuration-groups': configurationGroupsData,
    ...(outputsData &&
      Object.keys(outputsData).length > 0 && {
        outputs: [outputsData],
      }),
  };

  return config;
}

function downloadPluginJSON(widgetJSONData) {
  const JSONConfig = getJSONConfig(widgetJSONData);
  const { pluginName, pluginType } = widgetJSONData;
  fileDownload(JSON.stringify(JSONConfig, undefined, 4), `${pluginName}-${pluginType}.json`);
}

export { getJSONConfig, downloadPluginJSON };
