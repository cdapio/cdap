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
import { MyDataSyncApi } from 'api/datasync';
import { Actions } from 'components/DataSync/Create/store';
import { connect } from 'react-redux';
// import './ChooseSource.scss';

interface IProps {
  onClick: () => void;
}

interface IState {
  sources: any;
}

class ChooseSinkView extends React.PureComponent<IProps, IState> {
  public state = {
    sources: [],
  };

  public componentDidMount() {
    MyDataSyncApi.fetchSources().subscribe((sources) => {
      this.setState({
        sources,
      });
    });
  }

  public render() {
    return (
      <div>
        <h2>Select Sink</h2>
        <div className="choose-source">
          {this.state.sources.map((source, index) => {
            return (
              <div className="source-container" key={`${source.name}${index}`}>
                <div className="source" onClick={this.props.onClick.bind(null, source)}>
                  <h3 className="name">{source.name}</h3>
                  <div className="description">{source.description}</div>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    );
  }
}

const mapDispatch = (dispatch) => {
  return {
    onClick: (sink) => {
      dispatch({
        type: Actions.setSink,
        payload: {
          sink,
        },
      });
    },
  };
};

const ChooseSink = connect(
  null,
  mapDispatch
)(ChooseSinkView);

export default ChooseSink;
