import { IWidgetInfo } from 'components/PluginJSONCreator/CreateContextConnect';
import fileDownload from 'js-file-download';

function getJSONConfig(widgetJSONData) {
  const {
    pluginName,
    pluginType,
    displayName,
    emitAlerts,
    emitErrors,
    configurationGroups,
    groupToInfo,
    groupToWidgets,
    widgetToInfo,
  } = widgetJSONData;

  const configurationGroupsData = configurationGroups.map((groupID: string) => {
    const groupLabel = groupToInfo[groupID].label;
    const widgetData = groupToWidgets[groupID].map((widgetID: string) => {
      const widgetInfo: IWidgetInfo = widgetToInfo[widgetID];

      return {
        'widget-type': widgetInfo.widgetType,
        label: widgetInfo.label,
        name: widgetInfo.name,
        ...(widgetInfo.widgetCategory && { 'widget-category': widgetInfo.widgetCategory }),
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
    ...(pluginName && { 'plugin-name': pluginName }),
    ...(pluginType && { 'plugin-type': pluginType }),
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
