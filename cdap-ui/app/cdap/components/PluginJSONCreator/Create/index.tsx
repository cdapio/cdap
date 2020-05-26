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

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import Content from 'components/PluginJSONCreator/Create/Content';
import WizardGuideline from 'components/PluginJSONCreator/Create/WizardGuideline';
import * as React from 'react';

export const CreateContext = React.createContext({});
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
  };
};

interface ICreateState {
  activeStep: number;
  pluginName: string;
  pluginType: string;
  displayName: string;
  emitAlerts: boolean;
  emitErrors: boolean;
  configurationGroups: string[];
  groupToInfo: any;
  groupToWidgets: any;
  widgetToInfo: any;
  widgetToAttributes: any;
  outputName: string;
  // data for filter page
  filters: string[];
  filterToName: any;
  filterToCondition: any;
  filterToShowList: any;
  showToInfo: any;

  setActiveStep: (step: number) => void;
  setDisplayName: (displayName: string) => void;
  setBasicPluginInfo: (basicPluginInfo: IBasicPluginInfo) => void;
  setConfigurationGroups: (groups: string[]) => void;
  setGroupToInfo: (groupToInfo: any) => void;
  setGroupToWidgets: (groupToWidgets: any) => void;
  setWidgetToInfo: (widgetToInfo: any) => void;
  setWidgetToAttributes: (widgetToAttributes: any) => void;
  setOutputName: (outputName: string) => void;
  setFilters: (filters: string[]) => void;
  setFilterToName: (filterToName: any) => void;
  setFilterToCondition: (filterToCondition: any) => void;
  setFilterToShowList: (filterToShowList: any) => void;
  setShowToInfo: (showToInfo: any) => void;
}

export interface IBasicPluginInfo {
  pluginName: string;
  pluginType: string;
  displayName: string;
  emitAlerts: boolean;
  emitErrors: boolean;
}

export interface IConfigurationGroupInfo {
  label: string;
  description?: string;
}

export interface IWidgetInfo {
  name: string;
  label: string;
  widgetType: string;
  widgetCategory?: string;
}

/*export enum OutputSchemaType {
  Explicit = 'schema',
  Implicit = 'non-editable-schema-editor',
}*/

export type ICreateContext = Partial<ICreateState>;

class CreateView extends React.PureComponent<ICreateContext & WithStyles<typeof styles>> {
  public setActiveStep = (activeStep: number) => {
    this.setState({ activeStep });
  };

  public setDisplayName = (displayName: string) => {
    this.setState({ displayName });
  };

  public setBasicPluginInfo = (basicPluginInfo: IBasicPluginInfo) => {
    const { pluginName, pluginType, displayName, emitAlerts, emitErrors } = basicPluginInfo;
    this.setState({
      pluginName,
      pluginType,
      displayName,
      emitAlerts,
      emitErrors,
    });
  };

  public setConfigurationGroups = (configurationGroups: string[]) => {
    this.setState({ configurationGroups });
  };

  public setGroupToInfo = (groupToInfo: any) => {
    this.setState({ groupToInfo });
  };

  public setGroupToWidgets = (groupToWidgets: any) => {
    this.setState({ groupToWidgets });
  };

  public setWidgetToInfo = (widgetToInfo: any) => {
    this.setState({ widgetToInfo });
  };

  public setWidgetToAttributes = (widgetToAttributes: any) => {
    this.setState({ widgetToAttributes });
  };

  public setOutputName = (outputName: string) => {
    this.setState({ outputName });
  };

  public setFilters = (filters: string[]) => {
    this.setState({ filters });
  };

  public setFilterToName = (filterToName: any) => {
    this.setState({ filterToName });
  };

  public setFilterToCondition = (filterToCondition: any) => {
    this.setState({ filterToCondition });
  };

  public setFilterToShowList = (filterToShowList: any) => {
    this.setState({ filterToShowList });
  };

  public setShowToInfo = (showToInfo: any) => {
    this.setState({ showToInfo });
  };

  public state = {
    activeStep: 0,
    pluginName: '',
    pluginType: '',
    displayName: '',
    emitAlerts: true,
    emitErrors: true,
    configurationGroups: [],
    groupToInfo: {},
    groupToWidgets: {},
    widgetToInfo: {},
    widgetToAttributes: {},
    // outputSchemaType: OutputSchemaType.Explicit,
    // schemaTypes: [],
    filters: [],
    filterToName: {},
    filterToCondition: {},
    filterToShowList: {},
    showToInfo: {},

    setActiveStep: this.setActiveStep,
    setDisplayName: this.setDisplayName,
    setBasicPluginInfo: this.setBasicPluginInfo,
    setConfigurationGroups: this.setConfigurationGroups,
    setGroupToInfo: this.setGroupToInfo,
    setGroupToWidgets: this.setGroupToWidgets,
    setWidgetToInfo: this.setWidgetToInfo,
    setWidgetToAttributes: this.setWidgetToAttributes,
    setOutputName: this.setOutputName,
    setFilters: this.setFilters,
    setFilterToName: this.setFilterToName,
    setFilterToCondition: this.setFilterToCondition,
    setFilterToShowList: this.setFilterToShowList,
    setShowToInfo: this.setShowToInfo,
  };

  public render() {
    return (
      <CreateContext.Provider value={this.state}>
        <div className={this.props.classes.root}>
          <div className={this.props.classes.content}>
            <WizardGuideline />
            <Content />
          </div>
        </div>
      </CreateContext.Provider>
    );
  }
}

export function createContextConnect(Comp) {
  return (extraProps) => {
    return (
      <CreateContext.Consumer>
        {(props) => {
          const finalProps = {
            ...props,
            ...extraProps,
          };

          return <Comp {...finalProps} />;
        }}
      </CreateContext.Consumer>
    );
  };
}

const Create = withStyles(styles)(CreateView);
export default Create;
