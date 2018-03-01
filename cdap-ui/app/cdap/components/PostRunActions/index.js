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

import React, { Component } from 'react';
import PropTypes from 'prop-types';
import PostRunActionsWizard from 'components/PostRunActions/PostRunActionsWizard';

require('./PostRunActions.scss');

export default class PostRunActions extends Component {
  static propTypes = {
    actions: PropTypes.array
  };

  state = {
    activeActionWizard: null
  }

  setActiveActionWizard = (action = null) => {
    this.setState({
      activeActionWizard: action
    });
  }

  render() {
    if (!this.props.actions.length) {
      return (
        <div className="post-run-actions-table well well-sm text-center empty-table disabled">
          <h2>No alerts configured for this pipeline</h2>
        </div>
      );
    }

    return (
      <div className="post-run-actions-table">
        <h2>Saved Alerts</h2>
        <table className="table table-bordered">
          <thead>
            <tr>
              <th>Alert</th>
              <th>Event</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {
              this.props.actions.map(action => {
                return (
                  <tr>
                    <td>{action.plugin.name}</td>
                    <td>{action.plugin.properties.runCondition}</td>
                    <td className="text-center">
                      <a onClick={this.setActiveActionWizard.bind(this, action)}>
                        View
                      </a>
                    </td>
                  </tr>
                );
              })
            }
          </tbody>
        </table>
        {
          this.state.activeActionWizard !== null ?
            (
              <PostRunActionsWizard
                action={this.state.activeActionWizard}
                isOpen={this.state.activeActionWizard !== null}
                toggleModal={this.setActiveActionWizard}
              />
            )
          :
            null
        }

      </div>
    );
  }
}
