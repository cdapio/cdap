/*
 * Copyright © 2017 Cask Data, Inc.
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
import classnames from 'classnames';

export default class DirectivesTabRow extends Component {
  constructor(props) {
    super(props);

    this.state = {
      expandable: false,
      isExpanded: false,
    };

    this.toggleExpand = this.toggleExpand.bind(this);
  }

  componentDidMount() {
    let container = document.getElementById(`name-container-${this.props.rowInfo.uniqueId}`);
    let content = document.getElementById(`name-content-${this.props.rowInfo.uniqueId}`);

    if (content.offsetWidth > container.offsetWidth) {
      this.setState({
        expandable: true,
      });
    }
  }

  toggleExpand() {
    this.setState({ isExpanded: !this.state.isExpanded });
  }

  render() {
    return (
      <div
        className={classnames('directives-row', {
          inactive: this.props.isInactive,
        })}
        data-cy={`directive-row-${this.props.rowIndex}`}
      >
        <span>{this.props.rowIndex + 1}</span>
        <span
          id={`name-container-${this.props.rowInfo.uniqueId}`}
          className={classnames({
            truncate: !this.state.isExpanded,
            expandable: this.state.expandable,
          })}
          onClick={this.toggleExpand}
        >
          <span id={`name-content-${this.props.rowInfo.uniqueId}`}>{this.props.rowInfo.name}</span>
        </span>
        <span className="float-right">
          <span
            className="fa fa-times"
            onClick={this.props.handleDelete}
            onMouseEnter={this.props.handleMouseEnter}
            onMouseLeave={this.props.handleMouseLeave}
            data-cy="delete-directive"
          />
        </span>
      </div>
    );
  }
}

DirectivesTabRow.propTypes = {
  rowInfo: PropTypes.object,
  rowIndex: PropTypes.number,
  isInactive: PropTypes.bool,
  handleDelete: PropTypes.func,
  handleMouseEnter: PropTypes.func,
  handleMouseLeave: PropTypes.func,
};
