<<<<<<< HEAD
import { SPEC_VERSION } from 'components/PluginJSONCreator/constants';
=======
>>>>>>> b68b04d60ed... [CDAP-16863] Show the live JSON view (plugin JSON Creator)
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
<<<<<<< HEAD
    widgetInfo,
    widgetToAttributes,
=======
    widgetToInfo,
>>>>>>> b68b04d60ed... [CDAP-16863] Show the live JSON view (plugin JSON Creator)
  } = widgetJSONData;

  const configurationGroupsData = configurationGroups.map((groupID: string) => {
    const groupLabel = groupToInfo[groupID].label;
    const widgetData = groupToWidgets[groupID].map((widgetID: string) => {
<<<<<<< HEAD
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
=======
      const widgetInfo: IWidgetInfo = widgetToInfo[widgetID];

      return {
        'widget-type': widgetInfo.widgetType,
        label: widgetInfo.label,
        name: widgetInfo.name,
        ...(widgetInfo.widgetCategory && { 'widget-category': widgetInfo.widgetCategory }),
>>>>>>> b68b04d60ed... [CDAP-16863] Show the live JSON view (plugin JSON Creator)
      };
    });
    return {
      label: groupLabel,
      properties: widgetData,
    };
  });

  const config = {
    metadata: {
<<<<<<< HEAD
      'spec-version': SPEC_VERSION,
=======
      'spec-version': '1.5',
>>>>>>> b68b04d60ed... [CDAP-16863] Show the live JSON view (plugin JSON Creator)
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
