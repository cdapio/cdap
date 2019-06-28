/*
 * Copyright © 2019 Cask Data, Inc.
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
import {
  TransfersCreateContext,
  defaultContext,
  Stages,
} from 'components/Transfers/Create/context';
import StepContent from 'components/Transfers/Create/StepContent';
import NameDescription from 'components/Transfers/Create/Configure/NameDescription';
import SourceConfig from 'components/Transfers/Create/Configure/SourceConfig';
import TargetConfig from 'components/Transfers/Create/Configure/TargetConfig';
import LeftPanel from 'components/Transfers/Create/LeftPanel';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import Source from '../Configure/PluginPicker/Source';
import Target from '../Configure/PluginPicker/Target';
import GenerateAssessment from '../Assessment/GenerateAssessment';
import ViewAssessment from '../Assessment/ViewAssessment';
import ViewSummary from '../Publish/ViewSummary';
import ConfigureSummary from '../Configure/Summary';
import { MyDeltaApi } from 'api/delta';
import { getCurrentNamespace } from 'services/NamespaceStore';

const styles = (): StyleRules => {
  return {
    root: {
      display: 'flex',
      height: 'calc(100% - 50px)',
    },
  };
};

export const StageConfiguration = {
  [Stages.CONFIGURE]: {
    label: 'Configure',
    steps: [
      {
        label: 'Set a name and description',
        component: NameDescription,
      },
      {
        label: 'Choose a source',
        component: Source,
      },
      {
        label: 'Set source configs',
        component: SourceConfig,
      },
      {
        label: 'Choose a target',
        component: Target,
      },
      {
        label: 'Set target configs',
        component: TargetConfig,
      },
      {
        label: `Review configuration`,
        component: ConfigureSummary,
      },
    ],
  },
  [Stages.ASSESSMENT]: {
    label: 'Assessment',
    steps: [
      {
        label: 'Generate assessment',
        component: GenerateAssessment,
      },
      {
        label: 'View assessment',
        component: ViewAssessment,
      },
    ],
  },
  [Stages.PUBLISH]: {
    label: 'Publish',
    steps: [
      {
        label: 'View summary',
        component: ViewSummary,
      },
    ],
  },
};

interface IContentProps extends WithStyles<typeof styles> {
  id: string;
}

class ContentView extends React.PureComponent<IContentProps, typeof defaultContext> {
  public componentDidMount() {
    const id = this.props.id;
    if (!id) {
      return;
    }

    // initialize state if id is present
    const params = {
      context: getCurrentNamespace(),
      id,
    };
    MyDeltaApi.get(params).subscribe(
      (res) => {
        this.setState({
          id: res.id,
          name: res.name,
          description: res.description,
          ...res.properties,
        });
      },
      (err) => {
        // tslint:disable-next-line:no-console
        console.log('Error fetching instance', err);
      }
    );
  }

  public getRequestBody = (activeStep) => {
    const requestBody = {
      name: this.state.name,
      description: this.state.description,
      properties: {
        stage: this.state.stage,
        activeStep,
        sourceConfig: this.state.sourceConfig,
        source: this.state.source,
        targetConfig: this.state.targetConfig,
        target: this.state.target,
      },
    };

    return requestBody;
  };

  private updateStore = (activeStep = this.state.activeStep) => {
    if (!this.state.id) {
      return;
    }
    const params = {
      context: getCurrentNamespace(),
      id: this.state.id,
    };

    const requestBody = this.getRequestBody;

    MyDeltaApi.update(params, requestBody).subscribe(
      (res) => {
        // tslint:disable-next-line:no-console
        console.log('res', res);
      },
      (err) => {
        // tslint:disable-next-line:no-console
        console.log('error', err);
      }
    );
  };

  public next = (updateStore = true) => {
    if (updateStore) {
      this.updateStore(this.state.activeStep + 1);
    }

    this.setState({
      activeStep: this.state.activeStep + 1,
    });
  };

  public previous = () => {
    this.setState({
      activeStep: this.state.activeStep - 1,
    });
  };

  public setActiveStep = (step) => {
    this.setState({
      activeStep: step,
    });
  };

  public setNameDescription = (id, name, description) => {
    this.setState({
      id,
      name,
      description,
    });
  };

  public setSource = (source, sourceConfig) => {
    this.setState({
      source,
      sourceConfig,
    });
  };

  public setTarget = (target, targetConfig) => {
    this.setState({
      target,
      targetConfig,
    });
  };

  public setStage = (stage) => {
    this.setState(
      {
        stage,
        activeStep: 0,
      },
      () => {
        this.updateStore(0);
      }
    );
  };

  public state = {
    ...defaultContext,
    next: this.next,
    previous: this.previous,
    setNameDescription: this.setNameDescription,
    setSource: this.setSource,
    setTarget: this.setTarget,
    setActiveStep: this.setActiveStep,
    setStage: this.setStage,
    getRequestBody: this.getRequestBody,
  };

  public render() {
    return (
      <TransfersCreateContext.Provider value={this.state}>
        <div className={this.props.classes.root}>
          <LeftPanel />
          <StepContent />
        </div>
      </TransfersCreateContext.Provider>
    );
  }
}

const Content = withStyles(styles)(ContentView);
export default Content;
