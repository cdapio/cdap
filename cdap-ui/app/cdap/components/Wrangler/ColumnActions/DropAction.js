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

import React, { Component, PropTypes } from 'react';
import WranglerActions from 'components/Wrangler/Store/WranglerActions';
import WranglerStore from 'components/Wrangler/Store/WranglerStore';

export default class DropAction extends Component {
  constructor(props) {
    super(props);

    this.onClick = this.onClick.bind(this);
  }

  onClick() {
    WranglerStore.dispatch({
      type: WranglerActions.dropColumn,
      payload: {
        activeColumn: this.props.column
      }
    });
  }

  render() {
    return (
      <span className="column-actions drop-action">
        <span
          className="fa fa-trash"
          onClick={this.onClick}
        />
      </span>
    );
  }
}

DropAction.propTypes = {
  column: PropTypes.string
};
