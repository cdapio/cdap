/*
 * Copyright © 2016 Cask Data, Inc.
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

import React, {Component, PropTypes} from 'react';
import NamespaceStore from 'services/NamespaceStore';
import {MyProgramApi} from 'api/program';
import FastActionButton from '../FastActionButton';
import {convertProgramToApi} from 'services/program-api-converter';

export default class StartStopAction extends Component {
  constructor(props) {
    super(props);

    this.params = {
      namespace: NamespaceStore.getState().selectedNamespace,
      appId: this.props.entity.applicationId,
      programType: convertProgramToApi(this.props.entity.programType),
      programId: this.props.entity.id
    };

    this.state = {
      status: 'loading'
    };
  }

  componentWillMount() {
    this.statusPoll$ = MyProgramApi.pollStatus(this.params)
      .subscribe((res) => {
        this.setState({status: res.status});
      });
  }

  componentWillUnmount() {
    this.statusPoll$.dispose();
  }

  onClick(event) {
    let params = Object.assign({}, this.params);
    if (this.state.status === 'RUNNING' || this.state.status === 'STARTING') {
      params.action = 'stop';
    } else {
      params.action = 'start';
    }

    this.setState({status: 'loading'});

    MyProgramApi.action(params)
      .subscribe(this.props.onSuccess, (err) => {
        alert(err);
      });
    event.stopPropagation();
    event.nativeEvent.stopImmediatePropagation();
  }

  render() {
    if (this.state.status === 'loading') {
      return (
        <button className="btn btn-link" disabled>
          <span className="fa fa-spin fa-spinner"></span>
        </button>
      );
    }

    let icon;
    if (this.state.status === 'RUNNING' || this.state.status === 'STARTING') {
      icon = 'fa fa-stop text-danger';
    } else {
      icon = 'fa fa-play text-success';
    }

    return (
      <FastActionButton
        icon={icon}
        action={this.onClick.bind(this)}
      />
    );
  }
}

StartStopAction.propTypes = {
  entity: PropTypes.shape({
    id: PropTypes.string.isRequired,
    applicationId: PropTypes.string.isRequired,
    programType: PropTypes.string.isRequired
  }),
  onSuccess: PropTypes.func
};
