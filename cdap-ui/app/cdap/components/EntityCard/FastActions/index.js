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
import FastAction from 'components/FastAction';

export default class FastActions extends Component {
  constructor(props) {
    super(props);
  }

  listOfFastActions() {
    let fastActionTypes = [];

    switch (this.props.entity.type) {
      case 'artifact':
        fastActionTypes = ['delete'];
        break;
      case 'application':
        fastActionTypes = ['delete'];
        break;
      case 'stream':
        fastActionTypes = ['truncate', 'delete', 'explore', 'sendEvents'];
        break;
      case 'datasetinstance':
        fastActionTypes = ['truncate', 'delete', 'explore'];
        break;
      case 'program':
        fastActionTypes = ['startStop'];
        break;
    }

    return fastActionTypes;
  }

  onSuccess(action) {
    if (action === 'startStop') { return; }

    this.props.onUpdate();
  }

  render () {
    const fastActions = this.listOfFastActions();

    return (
      <h4 className="text-center">
        <span>
          {
            fastActions.map((action) => {
              return (
                <FastAction
                  key={action}
                  type={action}
                  entity={this.props.entity}
                  onSuccess={this.onSuccess.bind(this, action)}
                />
              );
            })
          }
        </span>
      </h4>
    );
  }
}

FastActions.propTypes = {
  entity: PropTypes.object,
  onUpdate: PropTypes.func
};
