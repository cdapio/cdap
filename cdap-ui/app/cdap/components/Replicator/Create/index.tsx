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
import LeftPanel from 'components/Replicator/Create/LeftPanel';
import EntityTopPanel from 'components/EntityTopPanel';
import Content from 'components/Replicator/Create/Content';
import { Redirect } from 'react-router-dom';
import { objectQuery } from 'services/helpers';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { fetchPluginInfo } from 'components/Replicator/utilities';
import { PluginType } from 'components/Replicator/constants';
import LoadingSVGCentered from 'components/LoadingSVGCentered';

export const CreateContext = React.createContext({});

const styles = (): StyleRules => {
  return {
    root: {
      height: '100%',
    },
    content: {
      height: 'calc(100% - 50px)',
      display: 'grid',
      gridTemplateColumns: '250px 1fr',

      '& > div': {
        overflowY: 'auto',
      },
    },
  };
};

interface ICreateProps extends WithStyles<typeof styles> {
  match: {
    params: {
      artifactName: string;
      artifactVersion: string;
      artifactScope: string;
      pluginNam: string;
    };
  };
}

type IPluginConfig = Record<string, string>;

interface ICreateState {
  name: string;
  description: string;
  sourcePlugin: any;
  targetPlugin: any;
  sourceConfig: IPluginConfig;
  targetConfig: IPluginConfig;
  isInvalidSource: boolean;
  loading: boolean;
  activeStep: number;
  setActiveStep: (step: number) => void;
  setNameDescription: (name: string, description?: string) => void;
  setSourceConfig: (sourceConfig: IPluginConfig) => void;
  setTargetPlugin: (targetPlugin: any) => void;
  setTargetConfig: (targetConfig: IPluginConfig) => void;
}

export type ICreateContext = Partial<ICreateState>;

class CreateView extends React.PureComponent<ICreateProps, ICreateContext> {
  public setActiveStep = (step: number) => {
    this.setState({ activeStep: step });
  };

  public setNameDescription = (name, description) => {
    this.setState({ name, description });
  };

  public setSourceConfig = (sourceConfig) => {
    this.setState({ sourceConfig });
  };

  public setTargetPlugin = (targetPlugin) => {
    this.setState({ targetPlugin });
  };

  public setTargetConfig = (targetConfig) => {
    this.setState({ targetConfig });
  };

  public state = {
    name: '',
    description: '',
    sourcePlugin: null,
    targetPlugin: null,
    sourceConfig: null,
    targetConfig: null,

    isInvalidSource: false,
    loading: true,

    activeStep: 0,

    setActiveStep: this.setActiveStep,
    setNameDescription: this.setNameDescription,
    setSourceConfig: this.setSourceConfig,
    setTargetPlugin: this.setTargetPlugin,
    setTargetConfig: this.setTargetConfig,
  };

  public componentDidMount() {
    // Set source
    const artifactName = objectQuery(this.props, 'match', 'params', 'artifactName');
    const artifactVersion = objectQuery(this.props, 'match', 'params', 'artifactVersion');
    const artifactScope = objectQuery(this.props, 'match', 'params', 'artifactScope');
    const pluginName = objectQuery(this.props, 'match', 'params', 'pluginName');

    if (!artifactName || !artifactVersion || !artifactScope || !pluginName) {
      this.setState({ isInvalidSource: true });
      return;
    }

    fetchPluginInfo(artifactName, artifactScope, pluginName, PluginType.source).subscribe(
      (res) => {
        this.setState({ sourcePlugin: res, loading: false });
      },
      (err) => {
        // tslint:disable-next-line: no-console
        console.error('Error fetching plugin', err);
        this.setState({ isInvalidSource: true });
      }
    );
  }

  private redirectToListView = () => {
    return <Redirect to={`/ns/${getCurrentNamespace()}/replicator`} />;
  };

  // private renderState = () => {
  //   const state = { ...this.state };

  //   Object.keys(state).forEach((stateKey) => {
  //     if (typeof state[stateKey] === 'function') {
  //       delete state[stateKey];
  //     }
  //   });

  //   return <pre>{JSON.stringify(state, null, 2)}</pre>;
  // };

  public render() {
    if (this.state.isInvalidSource) {
      return this.redirectToListView();
    }

    if (this.state.loading) {
      return <LoadingSVGCentered />;
    }

    return (
      <CreateContext.Provider value={this.state}>
        <div className={this.props.classes.root}>
          <EntityTopPanel title="Create new Replicator" closeBtnAnchorLink={() => history.back()} />
          <div className={this.props.classes.content}>
            <LeftPanel />
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
