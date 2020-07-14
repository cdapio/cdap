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

import withStyles, { StyleRules } from '@material-ui/core/styles/withStyles';
import classnames from 'classnames';
import { JSONStatusMessage, SchemaType } from 'components/PluginJSONCreator/constants';
import Content from 'components/PluginJSONCreator/Create/Content';
import PluginJSONMenu from 'components/PluginJSONCreator/Create/PluginJSONMenu';
import StepsGuidelineMenu from 'components/PluginJSONCreator/Create/StepsGuidelineMenu';
import { List, Map } from 'immutable';
import * as React from 'react';
import Dropzone from 'react-dropzone';

export const LEFT_PANEL_WIDTH = 250;

const styles = (): StyleRules => {
  return {
    root: {
      height: '100%',
    },
    content: {
      height: 'calc(100% - 50px)',
      display: 'grid',
      gridTemplateColumns: `${LEFT_PANEL_WIDTH}px 1fr`,

      '& > div': {
        overflowY: 'auto',
      },
    },
    overlayScreen: {
      position: 'fixed',
      padding: 0,
      margin: 0,
      width: '100%',
      color: 'rgba(0, 0, 0, 0.5)',
    },
  };
};

// Define initial values for states within Plugin Information page
const pluginInfoInitialState = {
  pluginName: '',
  setPluginName: undefined,
  pluginType: '',
  setPluginType: undefined,
  displayName: '',
  setDisplayName: undefined,
  emitAlerts: false,
  setEmitAlerts: undefined,
  emitErrors: false,
  setEmitErrors: undefined,
};

// Define initial values for states within Configuraiton Groups page
const configurationGroupsInitialState = {
  configurationGroups: List<string>(),
  setConfigurationGroups: undefined,
  groupToInfo: Map<string, Map<string, string>>(),
  setGroupToInfo: undefined,
};

// Define initial values for 'widgets' states within Configuraiton Groups page
const widgetsInitialState = {
  groupToWidgets: Map<string, List<string>>(),
  setGroupToWidgets: undefined,
  widgetInfo: Map<string, Map<string, string>>(),
  setWidgetInfo: undefined,
  widgetToAttributes: Map<string, Map<string, any>>(),
  setWidgetToAttributes: undefined,
};

// Define initial values for states within Outputs page
const outputInitialState = {
  outputName: '',
  setOutputName: undefined,
  outputWidgetType: SchemaType.Explicit,
  setOutputWidgetType: undefined,
  schemaTypes: [],
  setSchemaTypes: undefined,
  schemaDefaultType: '',
  setSchemaDefaultType: undefined,
  schema: {},
  setSchema: undefined,
};

// Define initial values for states within Filter Page
const filtersInitialState = {
  filters: List<string>(),
  setFilters: undefined,
  filterToName: Map<string, string>(),
  setFilterToName: undefined,
  filterToCondition: Map<string, Map<string, string>>(),
  setFilterToCondition: undefined,
  filterToShowlist: Map<string, List<string>>(),
  setFilterToShowlist: undefined,
  showToInfo: Map<string, Map<string, string>>(),
  setShowToInfo: undefined,
};

// Define initial values for internal states within the entire page
const appInternalInitialState = {
  activeStep: 0,
  setActiveStep: undefined,
  JSONStatus: JSONStatusMessage.Normal,
  setJSONStatus: undefined,
};

export const PluginInfoContext = React.createContext(pluginInfoInitialState);
export const ConfigurationGroupContext = React.createContext(configurationGroupsInitialState);
export const WidgetContext = React.createContext(widgetsInitialState);
export const OutputContext = React.createContext(outputInitialState);
export const FilterContext = React.createContext(filtersInitialState);
export const AppInternalContext = React.createContext(appInternalInitialState);

// Components within pluginJSONCreator will be able to use only the contexts that they choose.
// This will prevent unnecessary re-renderings of the UI.
export const usePluginInfoState = () => React.useContext(PluginInfoContext);
export const useConfigurationGroupState = () => React.useContext(ConfigurationGroupContext);
export const useWidgetState = () => React.useContext(WidgetContext);
export const useOutputState = () => React.useContext(OutputContext);
export const useFilterState = () => React.useContext(FilterContext);
export const useAppInternalState = () => React.useContext(AppInternalContext);

const GlobalStateProvider = ({
  children,
  pluginInfoContextValue,
  configuratioGroupContextValue,
  widgetContextValue,
  outputContextValue,
  filterContextValue,
  appInternalContextValue,
}) => {
  return (
    <PluginInfoContext.Provider value={pluginInfoContextValue}>
      <ConfigurationGroupContext.Provider value={configuratioGroupContextValue}>
        <WidgetContext.Provider value={widgetContextValue}>
          <OutputContext.Provider value={outputContextValue}>
            <FilterContext.Provider value={filterContextValue}>
              <AppInternalContext.Provider value={appInternalContextValue}>
                {children}
              </AppInternalContext.Provider>
            </FilterContext.Provider>
          </OutputContext.Provider>
        </WidgetContext.Provider>
      </ConfigurationGroupContext.Provider>
    </PluginInfoContext.Provider>
  );
};

const CreateView = ({ classes, children }) => {
  // Define states related to Basic Plugin Information page
  const [pluginName, setPluginName] = React.useState(pluginInfoInitialState.pluginName);
  const [pluginType, setPluginType] = React.useState(pluginInfoInitialState.pluginType);
  const [displayName, setDisplayName] = React.useState(pluginInfoInitialState.displayName);
  const [emitAlerts, setEmitAlerts] = React.useState(pluginInfoInitialState.emitAlerts);
  const [emitErrors, setEmitErrors] = React.useState(pluginInfoInitialState.emitErrors);

  // Define states related to Configuration Groups page
  const [configurationGroups, setConfigurationGroups] = React.useState(
    configurationGroupsInitialState.configurationGroups
  );
  const [groupToInfo, setGroupToInfo] = React.useState(configurationGroupsInitialState.groupToInfo);

  // Define states related to widgets in Configuraiton Groups page
  const [groupToWidgets, setGroupToWidgets] = React.useState(widgetsInitialState.groupToWidgets);
  const [widgetInfo, setWidgetInfo] = React.useState(widgetsInitialState.widgetInfo);
  const [widgetToAttributes, setWidgetToAttributes] = React.useState(
    widgetsInitialState.widgetToAttributes
  );

  // Define states related to Output page
  const [outputName, setOutputName] = React.useState(outputInitialState.outputName);
  const [outputWidgetType, setOutputWidgetType] = React.useState(
    outputInitialState.outputWidgetType
  );
  const [schemaTypes, setSchemaTypes] = React.useState(outputInitialState.schemaTypes);
  const [schemaDefaultType, setSchemaDefaultType] = React.useState(
    outputInitialState.schemaDefaultType
  );
  const [schema, setSchema] = React.useState(outputInitialState.schema);

  // Define states related to Filter page
  const [filters, setFilters] = React.useState(filtersInitialState.filters);
  const [filterToName, setFilterToName] = React.useState(filtersInitialState.filterToName);
  const [filterToCondition, setFilterToCondition] = React.useState(
    filtersInitialState.filterToCondition
  );
  const [filterToShowlist, setFilterToShowlist] = React.useState(
    filtersInitialState.filterToShowlist
  );
  const [showToInfo, setShowToInfo] = React.useState(filtersInitialState.showToInfo);

  // Define internal application states to manage pluginJSONCreator
  const [activeStep, setActiveStep] = React.useState(appInternalInitialState.activeStep);
  const [JSONStatus, setJSONStatus] = React.useState(appInternalInitialState.JSONStatus);

  const pluginInfoContextValue = React.useMemo(
    () => ({
      pluginName,
      setPluginName,
      pluginType,
      setPluginType,
      displayName,
      setDisplayName,
      emitAlerts,
      setEmitAlerts,
      emitErrors,
      setEmitErrors,
    }),
    [pluginName, pluginType, displayName, emitAlerts, emitErrors]
  );

  const configuratioGroupContextValue = React.useMemo(
    () => ({ configurationGroups, setConfigurationGroups, groupToInfo, setGroupToInfo }),
    [configurationGroups, groupToInfo]
  );

  const widgetContextValue = React.useMemo(
    () => ({
      groupToWidgets,
      setGroupToWidgets,
      widgetInfo,
      setWidgetInfo,
      widgetToAttributes,
      setWidgetToAttributes,
    }),
    [groupToWidgets, widgetInfo, widgetToAttributes]
  );

  const outputContextValue = React.useMemo(
    () => ({
      outputName,
      setOutputName,
      outputWidgetType,
      setOutputWidgetType,
      schemaTypes,
      setSchemaTypes,
      schemaDefaultType,
      setSchemaDefaultType,
      schema,
      setSchema,
    }),
    [outputName, outputWidgetType, schemaTypes, schemaDefaultType, schema]
  );

  const filterContextValue = React.useMemo(
    () => ({
      filters,
      setFilters,
      filterToName,
      setFilterToName,
      filterToCondition,
      setFilterToCondition,
      filterToShowlist,
      setFilterToShowlist,
      showToInfo,
      setShowToInfo,
    }),
    [filters, filterToName, filterToCondition, filterToShowlist, showToInfo]
  );

  const appInternalContextValue = React.useMemo(
    () => ({
      activeStep,
      setActiveStep,
      JSONStatus,
      setJSONStatus,
    }),
    [activeStep, setActiveStep, JSONStatus, setJSONStatus]
  );

  const [uploadedFile, setUploadedFile] = React.useState(null);

  // In the user drag-and-drops a file, it should process the uploaded file.
  const uploadFile = (acceptedFiles) => {
    const file = acceptedFiles[0];
    const filename = file.name;
    const filenameWithoutExtension = filename.substring(0, filename.lastIndexOf('.')) || filename;
    const reader = new FileReader();
    reader.readAsText(file);
    let fileContent;
    reader.onload = (r) => {
      fileContent = r.target.result;
      setUploadedFile({
        filename: filenameWithoutExtension,
        fileContent,
      });
    };
  };

  return (
    <Dropzone multiple={false} noClick onDropAccepted={uploadFile}>
      {({ getRootProps, getInputProps, isDragActive }) => (
        <div
          className={classnames({
            [classes.overlayScreen]: isDragActive,
          })}
        >
          <section>
            <div {...getRootProps()}>
              <input {...getInputProps()} />
              <GlobalStateProvider
                pluginInfoContextValue={pluginInfoContextValue}
                configuratioGroupContextValue={configuratioGroupContextValue}
                widgetContextValue={widgetContextValue}
                outputContextValue={outputContextValue}
                filterContextValue={filterContextValue}
                appInternalContextValue={appInternalContextValue}
              >
                <PluginJSONMenu uploadedFile={uploadedFile} />
              </GlobalStateProvider>
              <div className={classes.content}>
                <AppInternalContext.Provider value={appInternalContextValue}>
                  <StepsGuidelineMenu />
                  <Content
                    pluginInfoContextValue={pluginInfoContextValue}
                    configuratioGroupContextValue={configuratioGroupContextValue}
                    widgetContextValue={widgetContextValue}
                    outputContextValue={outputContextValue}
                    filterContextValue={filterContextValue}
                  />
                </AppInternalContext.Provider>
              </div>
            </div>
          </section>
        </div>
      )}
    </Dropzone>
  );
};

const Create = withStyles(styles)(CreateView);
export default Create;
