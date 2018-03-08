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
import {Popover, PopoverContent} from 'reactstrap';
import {isDescendant} from 'services/helpers';
import Mousetrap from 'mousetrap';
import {Observable} from 'rxjs/Observable';
import classnames from 'classnames';
import uuidV4 from 'uuid/v4';

export default class UncontrolledPopover extends Component {
  constructor(props) {
    super(props);
    this.state = {
      dropdownOpen: props.dropdownOpen,
      id: `popover-${uuidV4()}`
    };
    this.togglePopover = this.togglePopover.bind(this);
  }
  componentWillUnmount() {
    if (this.documentClick$) {
      this.documentClick$.unsubscribe();
    }
  }
  togglePopover() {
    let newState = !this.state.dropdownOpen;
    this.setState({
      dropdownOpen: newState
    });

    if (this.props.documentElement && newState) {
      this.documentClick$ = Observable.fromEvent(this.props.documentElement, 'click')
      .subscribe((e) => {
        if (isDescendant(this.popover, e.target) || !this.state.dropdownOpen) {
          return;
        }

        this.togglePopover();
      });
      Mousetrap.bind('esc', this.togglePopover);
    } else {
      if (this.documentClick$) {
        this.documentClick$.unsubscribe();
      }
      Mousetrap.unbind('esc');
    }
  }
  renderPopover() {
    let tetherOption = this.props.tetherOption || {};
    return (
      <Popover
        toggle={this.togglePopover}
        placement="bottom right"
        isOpen={this.state.dropdownOpen}
        target={this.state.id}
        className="dataprep-toggle-all-dropdown"
        tether={tetherOption}
      >
        <PopoverContent>
          {this.props.children}
        </PopoverContent>
      </Popover>
    );
  }
  render() {
    let iconName = this.props.icon || 'fa-caret-square-o-down';
    let Tag = this.props.tag || 'span';
    if (this.props.popoverElement) {
      return (
        <Tag
          id={this.state.id}
          className={this.props.className}
          onClick={this.togglePopover}
          ref={(ref) => this.popover = ref}
        >
          {this.props.popoverElement}
          {this.renderPopover()}
        </Tag>
      );
    }

    return (
      <span
        className={classnames(`fa ${iconName}`, this.props.className, {
          'expanded': this.state.dropdownOpen
        })}
        id={this.state.id}
        onClick={this.togglePopover}
        ref={(ref) => this.popover = ref}
      >
        {this.renderPopover()}
      </span>
    );
  }
}
UncontrolledPopover.propTypes = {
  tag: PropTypes.string,
  children: PropTypes.node.isRequired,
  popoverElement: PropTypes.node,
  dropdownOpen: PropTypes.bool,
  tetherOption: PropTypes.object,
  documentElement: PropTypes.node,
  icon: PropTypes.string,
  className: PropTypes.string
};
