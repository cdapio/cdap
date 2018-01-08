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
import React, {Component} from 'react';
import { Manager, Target, Popper, Arrow } from 'react-popper';
import {Observable} from 'rxjs/Observable';
import Mousetrap from 'mousetrap';
import classnames from 'classnames';
require('./Popover.scss');

export default class Popover extends Component {

  propTypes = {
    children: PropTypes.ReactElement,
    target: PropTypes.ReactElement,
    targetDimension: PropTypes.object.isRequired,
    className: PropTypes.string,
    placement: PropTypes.oneOf([
      'top',
      'bottom',
      'left',
      'right',
      'auto'
    ])
  };

  state = {
    showPopover: false
  };

  togglePopover = () => {
    let newState = !this.state.showPopover;

    this.setState({
      showPopover: newState
    });
    if (newState) {
      this.documentClick$ = Observable.fromEvent(document, 'click')
        .subscribe(() => {
          this.togglePopover();
        });

      Mousetrap.bind('esc', this.togglePopover);
    } else {
      if (this.documentClick$) {
        this.documentClick$.unsubscribe();
      }
      Mousetrap.unbind('esc');
    }
  };

  render() {
    const TargetElement = this.props.target;
    return (
      <Manager className={this.props.className}>
        <Target style={this.props.targetDimension} onClick={this.togglePopover}>
          <TargetElement />
        </Target>
        <Popper
          placement={this.props.placement || 'auto'}
          className={classnames("popper", {
          'hide': !this.state.showPopover
          })}
        >
          {this.props.children}
          <Arrow className="popper__arrow" />
        </Popper>
      </Manager>
    );
  }
}
