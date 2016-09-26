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
import {MyAppApi} from '../../../api/app';
import {MyArtifactApi} from '../../../api/artifact';

export default class ArtifactMetrics extends Component {
  constructor(props) {
    super(props);

    this.state = {
      extensions: 0,
      apps: 0,
      type: '-',
      loading: true
    };
  }

  componentWillMount() {
    const extensionParams = {
      namespace: 'default',
      artifactId: this.props.entity.id,
      version: this.props.entity.version,
      scope: this.props.entity.scope
    };

    const appsParams = {
      namespace: 'default',
      artifactName: this.props.entity.id,
      artifactVersion: this.props.entity.version,
    };

    MyArtifactApi.listExtensions(extensionParams)
      .combineLatest(MyAppApi.getDeployedApp(appsParams), MyArtifactApi.get(extensionParams))
      .subscribe((res) => {
        this.setState({
          extensions: res[0].length,
          apps: res[1].length,
          type: res[2].classes.plugins.length > 0 ? 'Plugin' : 'App',
          loading: false
        });
      });
  }

  render () {
    const loading = <span className="fa fa-spin fa-spinner"></span>;

    return (
      <div className="metrics-container">
        <div className="metric-item">
          <p className="metric-header">Extensions</p>
          <p>{this.state.loading ? loading : this.state.extensions}</p>
        </div>
        <div className="metric-item">
          <p className="metric-header">Applications</p>
          <p>{this.state.loading ? loading : this.state.apps}</p>
        </div>
        <div className="metric-item">
          <p className="metric-header">Type</p>
          <p>{this.state.loading ? loading : this.state.type}</p>
        </div>
      </div>
    );
  }
}

ArtifactMetrics.propTypes = {
  entity: PropTypes.object
};
