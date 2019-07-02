import { WithStyles } from '@material-ui/core/styles/withStyles';
import { ConnectionType } from 'components/DataPrepConnections/ConnectionType';

interface IArtifact {
  name: string;
  version: string;
  scope: string;
}

interface IPluginProperties {
  [key: string]: string;
}

interface IPlugin {
  label: string;
  artifact: IArtifact;
  properties: IPluginProperties;
}

interface IPluginNode {
  name: string;
  plugin: IPlugin;
  type: string;
}
/**
 * TODO: We might need this in the future where we specify the pipeline
 * type as well in widget attributes. Right now only kafka allows to add
 * source and wrangler as part of both batch and realtime pipelines.
 *
 * When tomorrow we have connections where the source plugin has different
 * properties between batch and realtime pipeline then we need to generate the right
 * properties to autofill.
 */
interface IWidgetAttributes {
  connectionType: ConnectionType;
}

interface IPluginFunctionConfig {
  widget: string;
  label: string;
  'widget-attributes': IWidgetAttributes;
}

export {
  IArtifact,
  IPluginProperties,
  IPlugin,
  IPluginNode,
  IWidgetAttributes,
  IPluginFunctionConfig,
};
