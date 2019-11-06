/*
 * Copyright © 2018 Cask Data, Inc.
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
import IconSVG from 'components/IconSVG';
import { Observable } from 'rxjs/Observable';
import Duration from 'components/Duration';
import { GLOBALS } from 'services/global-constants';
import { IPipeline } from 'components/PipelineList/DeployedPipelineView/types';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { MyPipelineApi } from 'api/pipeline';

interface IProps {
  pipeline: IPipeline;
}

interface IState {
  loading: boolean;
  nextRun: null | number;
}

export default class NextRun extends React.PureComponent<IProps, IState> {
  private interval = null;

  public state = {
    loading: true,
    nextRun: null,
  };

  public componentDidMount() {
    if (this.props.pipeline.artifact.name === GLOBALS.etlDataStreams) {
      return;
    }

    // Interval only runs after the delay, so have to
    // initially call the function first
    this.getNextRun();
    this.interval = Observable.interval(30 * 1000).subscribe(this.getNextRun.bind(this));
  }

  public componentWillUnmount() {
    if (this.interval && this.interval.unsubscribe) {
      this.interval.unsubscribe();
    }
  }

  private getNextRun() {
    const namespace = getCurrentNamespace();
    const pipeline = this.props.pipeline;

    const params = {
      ...GLOBALS.programInfo[GLOBALS.etlDataPipeline],
      namespace,
      appId: pipeline.name,
    };

    MyPipelineApi.getNextRun(params).subscribe((res) => {
      this.setState({
        loading: false,
        nextRun: res.length ? res[0].time : null,
      });
    });
  }

  private renderContent() {
    if (
      this.props.pipeline.artifact.name === GLOBALS.etlDataStreams ||
      (!this.state.loading && !this.state.nextRun)
    ) {
      return <span>--</span>;
    }

    if (this.state.loading) {
      // we have fa-lg here because we use tags on the same table row which is using larger loading icon
      // If both happen to load in UI it will this discrepancy (md vs lg)
      return (
        <span className="fa fa-spin fa-lg">
          <IconSVG name="icon-spinner" />
        </span>
      );
    }

    return <Duration targetTime={this.state.nextRun} />;
  }

  public render() {
    return <div className="next-run">{this.renderContent()}</div>;
  }
}
