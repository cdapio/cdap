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

import PropTypes from 'prop-types';
import React, { Component } from 'react';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {MyPipelineApi} from 'api/pipeline';
import IconSVG from 'components/IconSVG';
import {Observable} from 'rxjs/Observable';
import Duration from 'components/Duration';

export default class NextRun extends Component {
  static propTypes = {
    pipelineInfo: PropTypes.object
  };

  componentWillMount() {
    if (this.props.pipelineInfo.type === 'Realtime') { return; }

    // Interval only runs after the delay, so have to
    // initiall call the function first
    this.getNextRun();
    this.interval = Observable.interval(30 * 1000)
      .subscribe(this.getNextRun.bind(this));
  }

  componentWillUnmount() {
    if (this.interval && this.interval.unsubscribe) {
      this.interval.unsubscribe();
    }
  }

  state = {
    loading: true,
    nextRun: null
  };

  getNextRun() {
    let namespace = getCurrentNamespace();
    let pipelineInfo = this.props.pipelineInfo;

    // nextruntime endpoint is not under app version yet
    let params = {
      namespace,
      appId: pipelineInfo.name,
      programType: 'workflows',
      // version: pipelineInfo.version,
      programName: 'DataPipelineWorkflow'
    };

    MyPipelineApi.getNextRun(params)
      .subscribe((res) => {
        this.setState({
          loading: false,
          nextRun: res.length ? res[0].time : null
        });
      });
  }

  render() {
    if (this.props.pipelineInfo.type === 'Realtime' || (!this.state.loading && !this.state.nextRun)) {
      return <span>--</span>;
    }

    if (this.state.loading) {
      return (
        <span className="fa fa-spin">
          <IconSVG name="icon-spinner" />
        </span>
      );
    }
    return <Duration targetTime={this.state.nextRun} />;
  }
}
