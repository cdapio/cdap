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
import { connect } from 'react-redux';
import { MyDataSyncApi } from 'api/datasync';
import TextField from '@material-ui/core/TextField';
import Button from '@material-ui/core/Button';
import { Actions } from 'components/DataSync/Create/store';

interface IProps {
  sink: any;
  input: any;
  onNext: (sourceConfig: any) => void;
}

interface IState {
  properties: any;
}

class SinkConfigView extends React.PureComponent<IProps, IState> {
  public state = {
    properties: [],
    input: {},
  };

  public componentDidMount() {
    const source = this.props.sink;

    const params = {
      pluginName: source.name,
      artifactName: source.artifact.name,
      artifactVersion: source.artifact.version,
      artifactScope: source.artifact.scope,
      limit: 1,
      order: 'DESC',
      scope: 'SYSTEM',
    };

    MyDataSyncApi.fetchPluginConfig(params).subscribe((res) => {
      const prop = res[0].properties;
      const keys = Object.keys(prop);
      const properties = [];

      keys.forEach((k) => {
        properties.push(prop[k]);
      });

      this.setState({
        properties,
      });
    });
  }

  private handleChange = (prop, e) => {
    this.setState({
      input: {
        ...this.state.input,
        [prop]: e.target.value,
      },
    });

    console.log('steta', this.state.input);
  };

  private handleNext = () => {
    const config = this.state.input;

    this.props.onNext(config);
  };

  public render() {
    return (
      <div>
        <h2>Configure Sink</h2>

        <div className="source-config-container">
          <div className="config">
            {this.state.properties.map((prop) => {
              return (
                <div className="property-row" key={prop.name}>
                  <TextField
                    label={prop.name}
                    value={this.state.input[prop.name]}
                    onChange={this.handleChange.bind(this, prop.name)}
                    margin="normal"
                    helperText={prop.description}
                    variant="outlined"
                  />
                </div>
              );
            })}

            <div className="action-button">
              <Button variant="contained" size="large" color="primary" onClick={this.handleNext}>
                Next
              </Button>
            </div>
          </div>
        </div>
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    sink: state.datasync.sink,
  };
};

const mapDispatch = (dispatch) => {
  return {
    onNext: (sourceConfig) => {
      dispatch({
        type: Actions.setSinkConfig,
        payload: {
          sinkConfig: sourceConfig,
        },
      });
    },
  };
};

const SinkConfig = connect(
  mapStateToProps,
  mapDispatch
)(SinkConfigView);

export default SinkConfig;
