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
import WranglerActions from 'wrangler/components/Wrangler/Store/WranglerActions';
import WranglerStore from 'wrangler/components/Wrangler/Store/WranglerStore';
import {Tooltip} from 'reactstrap';

export default class UpperCaseAction extends Component {
  constructor(props) {
    super(props);

    this.state = {
      tooltipOpen: false
    };

    this.onClick = this.onClick.bind(this);
    this.toggle = this.toggle.bind(this);
  }

  toggle() {
    this.setState({tooltipOpen: !this.state.tooltipOpen});
  }

  onClick() {
    WranglerStore.dispatch({
      type: WranglerActions.upperCaseColumn,
      payload: {
        activeColumn: this.props.column
      }
    });
  }

  render() {
    const id = 'column-action-uppercase';

    return (
      <span className="column-actions">
        <span
          id={id}
          onClick={this.onClick}
          className="fa icon-uppercase"
        />

        <Tooltip
          placement="top"
          isOpen={this.state.tooltipOpen}
          toggle={this.toggle}
          target={id}
          className="wrangler-tooltip"
          delay={0}
        >
          Uppercase
        </Tooltip>
      </span>
    );
  }
}

UpperCaseAction.propTypes = {
  column: PropTypes.string
};
