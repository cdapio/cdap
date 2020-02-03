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
import { fetchPluginInfo, fetchPluginWidget } from 'components/Replicator/utilities';
import { PluginType } from 'components/Replicator/constants';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import uuidV4 from 'uuid/v4';
import { MyReplicatorApi } from 'api/replicator';

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
  history;
}

type IPluginConfig = Record<string, string>;

interface ICreateState {
  name: string;
  description: string;
  sourcePluginInfo: any;
  sourcePluginWidget: any;
  targetPluginInfo: any;
  targetPluginWidget: any;
  sourceConfig: IPluginConfig;
  targetConfig: IPluginConfig;
  draftId: string;
  isInvalidSource: boolean;
  loading: boolean;
  activeStep: number;
  setActiveStep: (step: number) => void;
  setNameDescription: (name: string, description?: string) => void;
  setSourcePluginWidget: (sourcePluginWidget) => void;
  setSourceConfig: (sourceConfig: IPluginConfig) => void;
  setTargetPluginInfo: (targetPluginInfo: any) => void;
  setTargetPluginWidget: (targetPluginWidget) => void;
  setTargetConfig: (targetConfig: IPluginConfig) => void;
}

export type ICreateContext = Partial<ICreateState>;

class CreateView extends React.PureComponent<ICreateProps, ICreateContext> {
  public setActiveStep = (step: number) => {
    setTimeout(() => {
      this.saveDraft().subscribe(
        () => {
          this.setState({ activeStep: step });
        },
        (err) => {
          // tslint:disable-next-line: no-console
          console.log('Failed to save draft', err);
        }
      );
    }, 100);
  };

  public setNameDescription = (name, description) => {
    this.setState({ name, description }, () => {
      this.props.history.push(
        `/ns/${getCurrentNamespace()}/replicator/drafts/${this.state.draftId}`
      );
    });
  };

  public setSourcePluginWidget = (sourcePluginWidget) => {
    this.setState({ sourcePluginWidget });
  };

  public setSourceConfig = (sourceConfig) => {
    this.setState({ sourceConfig });
  };

  public setTargetPluginInfo = (targetPluginInfo) => {
    this.setState({ targetPluginInfo });
  };

  public setTargetPluginWidget = (targetPluginWidget) => {
    this.setState({ targetPluginWidget });
  };

  public setTargetConfig = (targetConfig) => {
    this.setState({ targetConfig });
  };

  public state = {
    name: '',
    description: '',
    sourcePluginInfo: null,
    sourcePluginWidget: null,
    targetPluginInfo: null,
    targetPluginWidget: null,
    sourceConfig: null,
    targetConfig: null,

    draftId: null,
    isInvalidSource: false,
    loading: true,

    activeStep: 2,

    setActiveStep: this.setActiveStep,
    setNameDescription: this.setNameDescription,
    setSourcePluginWidget: this.setSourcePluginWidget,
    setSourceConfig: this.setSourceConfig,
    setTargetPluginInfo: this.setTargetPluginInfo,
    setTargetPluginWidget: this.setTargetPluginWidget,
    setTargetConfig: this.setTargetConfig,
  };

  public componentDidMount() {
    const draftId = objectQuery(this.props, 'match', 'params', 'draftId');
    if (!draftId) {
      this.initCreate();
      return;
    }

    this.initDraft(draftId);
  }

  private initCreate = () => {
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
        this.setState({ sourcePluginInfo: res, loading: false, draftId: uuidV4() });
      },
      (err) => {
        // tslint:disable-next-line: no-console
        console.error('Error fetching plugin', err);
        this.setState({ isInvalidSource: true });
      }
    );
  };

  private initDraft = (draftId) => {
    const params = {
      namespace: getCurrentNamespace(),
      draftId,
    };

    MyReplicatorApi.getDraft(params).subscribe(async (res) => {
      const newState: Partial<ICreateState> = {
        draftId,
        loading: false,
        name: res.label,
        description: objectQuery(res, 'config', 'description') || '',
      };

      const stages = objectQuery(res, 'config', 'stages') || [];
      const source = stages.find((stage) => {
        const stageType = objectQuery(stage, 'plugin', 'type');
        return stageType === PluginType.source;
      });

      const target = stages.find((stage) => {
        const stageType = objectQuery(stage, 'plugin', 'type');
        return stageType === PluginType.target;
      });

      if (source) {
        const sourceArtifact = objectQuery(source, 'plugin', 'artifact') || {};

        const sourcePluginInfo = await fetchPluginInfo(
          sourceArtifact.name,
          sourceArtifact.scope,
          source.plugin.name,
          source.plugin.type
        ).toPromise();

        newState.sourcePluginInfo = sourcePluginInfo;
        newState.sourceConfig = objectQuery(source, 'plugin', 'properties') || {};
      }

      if (target) {
        const targetArtifact = objectQuery(target, 'plugin', 'artifact') || {};

        const targetPluginInfo = await fetchPluginInfo(
          targetArtifact.name,
          targetArtifact.scope,
          target.plugin.name,
          target.plugin.type
        ).toPromise();

        newState.targetPluginInfo = targetPluginInfo;
        newState.targetConfig = objectQuery(target, 'plugin', 'properties') || {};
      }

      this.setState(newState);
    });
  };

  private saveDraft = () => {
    const params = {
      namespace: getCurrentNamespace(),
      draftId: this.state.draftId,
    };

    const body = this.getDraftBody();
    return MyReplicatorApi.putDraft(params, body);
  };

  private constructStageSpec = (type) => {
    const pluginKey = `${type}PluginInfo`;
    const configKey = `${type}Config`;

    if (!this.state[pluginKey]) {
      return null;
    }

    const plugin = this.state[pluginKey];

    const stage = {
      name: plugin.name,
      plugin: {
        name: plugin.name,
        type: plugin.type,
        artifact: {
          ...plugin.artifact,
        },
        properties: {},
      },
    };

    const pluginProperties = this.state[configKey];
    if (pluginProperties) {
      stage.plugin.properties = { ...pluginProperties };
    }

    return stage;
  };

  // TODO: Refactor
  private getDraftBody = () => {
    const source = this.constructStageSpec('source');
    const target = this.constructStageSpec('target');

    const stages = [];
    if (source) {
      stages.push(source);
    }

    if (target) {
      stages.push(target);
    }

    const connections = [];

    if (source && target) {
      connections.push({
        from: source.name,
        to: target.name,
      });
    }

    const body = {
      label: this.state.name,
      config: {
        description: this.state.description,
        connections,
        stages,
      },
    };

    return body;
  };

  private redirectToListView = () => {
    return <Redirect to={`/ns/${getCurrentNamespace()}/replicator`} />;
  };

  private renderState = () => {
    const state = { ...this.state };

    Object.keys(state).forEach((stateKey) => {
      if (typeof state[stateKey] === 'function') {
        delete state[stateKey];
      }
    });

    return <pre>{JSON.stringify(state, null, 2)}</pre>;
  };

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
        {this.renderState()}
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
