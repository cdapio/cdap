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

import * as React from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import WizardGuideline from 'components/PluginJSONCreator/Create/WizardGuideline';
import Content from 'components/PluginJSONCreator/Create/Content';

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
  configurationGroups: IConfigurationGroup[];
  setActiveStep: (step: number) => void;
  setBasicPluginInfo: (basicPluginInfo: IBasicPluginInfo) => void;
  setConfigurationGroups: (groups: IConfigurationGroup[]) => void;
}

export interface IBasicPluginInfo {
  pluginName: string;
  pluginType: string;
  displayName: string;
  emitAlerts: boolean;
  emitErrors: boolean;
}

export interface IConfigurationGroup {
  label: string;
  description?: string;
}

export type ICreateContext = Partial<ICreateState>;

class CreateView extends React.PureComponent<ICreateContext & WithStyles<typeof styles>> {
  public setActiveStep = (activeStep: number) => {
    this.setState({ activeStep });
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

  public setConfigurationGroups = (configurationGroups: IConfigurationGroup[]) => {
    this.setState({ configurationGroups });
  };

  public state = {
    activeStep: 0,
    pluginName: '',
    pluginType: '',
    displayName: '',
    emitAlerts: true,
    emitErrors: true,
    configurationGroups: [],

    setActiveStep: this.setActiveStep,
    setBasicPluginInfo: this.setBasicPluginInfo,
    setConfigurationGroups: this.setConfigurationGroups,
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
