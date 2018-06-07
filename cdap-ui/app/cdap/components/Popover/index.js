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
import React, {PureComponent} from 'react';
import { Manager, Target, Popper, Arrow } from 'react-popper';
import {Observable} from 'rxjs/Observable';
import Mousetrap from 'mousetrap';
import classnames from 'classnames';
import {preventPropagation, isDescendant} from 'services/helpers';
import uuidV4 from 'uuid/v4';
import ee from 'event-emitter';
require('./Popover.scss');

export default class Popover extends PureComponent {

  static propTypes = {
    children: PropTypes.element,
    target: PropTypes.element,
    targetDimension: PropTypes.object.isRequired,
    className: PropTypes.string,
    showOn: PropTypes.oneOf(['Click', 'Hover']),
    bubbleEvent: PropTypes.bool,
    placement: PropTypes.oneOf([
      'top',
      'bottom',
      'left',
      'right',
      'auto'
    ]),
    enableInteractionInPopover: PropTypes.bool,
    injectOnToggle: PropTypes.bool,
    showPopover: PropTypes.bool,
    onTogglePopover: PropTypes.func,
    modifiers: PropTypes.object
  };

  eventEmitter = ee(ee);

  static defaultProps = {
    showOn: 'Click',
    bubbleEvent: true,
    enableInteractionInPopover: false,
    injectOnToggle: false,
    modifiers: {
      preventOverflow: {
        enabled: true,
        boundariesElement: 'scrollParent'
      }
    }
  };

  state = {
    showPopover: this.props.showPopover
  };

  id = `popover-${uuidV4()}`;

  hidePopoverEventHandler = (popoverId) => {
    if (this.id !== popoverId) {
      this.setState({
        showPopover: false
      }, this.updateParentOnToggle);
    }
  };

  componentDidMount() {
    this.eventEmitter.on('POPOVER_OPEN', this.hidePopoverEventHandler);
  }

  componentWillReceiveProps(nextProps) {
    if (typeof nextProps.showPopover === 'boolean' &&
        nextProps.showPopover !== this.state.showPopover) {
      this.togglePopover();
    }
  }

  componentWillUnmount() {
    this.eventEmitter.off('POPOVER_OPEN', this.hidePopoverEventHandler);
  }

  updateParentOnToggle = () => {
    if (this.props.onTogglePopover) {
      this.props.onTogglePopover(this.state.showPopover);
    }
  };

  cleanUpDocumentClickEventHandler = () => {
    if (this.documentClick$) {
      this.documentClick$.unsubscribe();
    }
    Mousetrap.unbind('esc');
  }
  togglePopover = (e) => {
    let newState = !this.state.showPopover;

    this.setState({
      showPopover: newState
    }, this.updateParentOnToggle);

    if (newState) {
      this.eventEmitter.emit('POPOVER_OPEN', this.id);
      this.documentClick$ = Observable.fromEvent(document, 'click')
        .subscribe((e) => {
          let parent = document.getElementById(this.id);
          let child = e.target;
          if (this.props.enableInteractionInPopover && isDescendant(parent, child)) {
            preventPropagation(e);
            return false;
          }
          this.cleanUpDocumentClickEventHandler();
          this.setState({
            showPopover: false
          }, this.updateParentOnToggle);
        });

      Mousetrap.bind('esc', this.togglePopover);
    } else {
      this.cleanUpDocumentClickEventHandler();
    }
    this.handleBubbleEvent(e);
  };
  handleBubbleEvent = (e) => {
    if (!this.props.bubbleEvent) {
      preventPropagation(e);
      return false;
    }
  }

  render() {
    let targetProps = {
      style: this.props.targetDimension
    };
    if (this.props.showOn === 'Click') {
      targetProps[`on${this.props.showOn}`] = this.togglePopover;
    } else if (this.props.showOn === 'Hover') {
      targetProps['onMouseOver'] = this.togglePopover;
      targetProps['onMouseOut'] = this.togglePopover;
    }
    const TargetElement = this.props.target;
    return (
      <Manager className={this.props.className}>
        <Target {...targetProps}>
          <TargetElement />
        </Target>
        <Popper
          id={this.id}
          placement={this.props.placement || 'auto'}
          className={classnames("popper", {
          'hide': !this.state.showPopover,
          'tooltip': this.props.showOn === 'Hover'
          })}
          modifiers={this.props.modifiers}
          onClick={this.handleBubbleEvent}
        >
          {
            this.props.injectOnToggle ?
              this.state.showPopover ? this.props.children : null
            :
              this.props.children
          }
          <Arrow className="popper__arrow" />
        </Popper>
      </Manager>
    );
  }
}
