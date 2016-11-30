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
require('./WrangleHistory.less');

export default class WrangleHistory extends Component {
  constructor(props) {
    super(props);

    this.state = {
      expanded: {}
    };

  }

  historyItemClick(id) {
    let newObj = {};
    newObj[id] = !this.state.expanded[id];

    let expanded = Object.assign({}, this.state.expanded, newObj);
    this.setState({expanded});
  }

  renderHistoryPayload(history) {
    if (!this.state.expanded[history.id]) {
      return null;
    }

    return (
      <pre>{JSON.stringify(history.payload, null, 2)}</pre>
    );
  }

  render() {
    return (
      <div className="wrangler-history">
        <div className="transform-item">
          <span className="fa fa-list-ol" />
          <span className="transform-item-text">History</span>
        </div>

        <div className="history-list">
          {
            this.props.historyArray.map((history) => {
              return (
                <div
                  className="history-row"
                  key={history.id}
                  onClick={this.historyItemClick.bind(this, history.id)}
                >
                  <span>
                    <span>{history.action}</span>
                    <span className="fa fa-times-circle pull-right"></span>
                  </span>
                  {this.renderHistoryPayload(history)}
                </div>
              );
            })
          }
        </div>
      </div>
    );
  }
}

WrangleHistory.defaultProps = {
  historyArray: []
};

WrangleHistory.propTypes = {
  historyArray: PropTypes.arrayOf(PropTypes.shape({
    id: PropTypes.string,
    action: PropTypes.string,
  }))
};
