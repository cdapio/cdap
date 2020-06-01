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
    widgetToInfo,
    widgetToAttributes,
  } = widgetJSONData;

  const configurationGroupsData = configurationGroups.map((groupID: string) => {
    const groupLabel = groupToInfo[groupID].label;
    const widgetData = groupToWidgets[groupID].map((widgetID: string) => {
      const widgetInfo: IWidgetInfo = widgetToInfo[widgetID];
      const widgetAttributes = widgetToAttributes[widgetID];

      return {
        'widget-type': widgetInfo.widgetType,
        label: widgetInfo.label,
        name: widgetInfo.name,
        ...(widgetInfo.widgetCategory && { 'widget-category': widgetInfo.widgetCategory }),
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

  const config = {
    metadata: {
      'spec-version': '1.5',
    },
    ...(displayName && { 'display-name': displayName }),
    ...(emitAlerts && { 'emit-alerts': emitAlerts }),
    ...(emitErrors && { 'emit-errors': emitErrors }),
    'configuration-groups': configurationGroupsData,
  };

  return config;
}

function downloadPluginJSON(widgetJSONData) {
  const JSONConfig = getJSONConfig(widgetJSONData);
  const { pluginName, pluginType } = widgetJSONData;
  fileDownload(JSON.stringify(JSONConfig, undefined, 4), `${pluginName}-${pluginType}.json`);
}

export { getJSONConfig, downloadPluginJSON };
