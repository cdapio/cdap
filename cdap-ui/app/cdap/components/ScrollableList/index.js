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

import React, { Component, PropTypes } from 'react';
import IconSVG from 'components/IconSVG';
import {difference} from 'services/helpers';
import findIndex from 'lodash/findIndex';

require('./ScrollableList.scss');

export default class ScrollableList extends Component {
  constructor(props) {
    super(props);
    this.state = {
      children: this.props.children,
      numberOfElemsInList: null,
      startingIndex: 0
    };
    this.computeHeight = this.computeHeight.bind(this);
    this.scrollUp = this.scrollUp.bind(this);
    this.scrollDown = this.scrollDown.bind(this);
  }
  componentDidMount() {
    this.computeHeight();
  }
  componentWillReceiveProps(nextProps) {
    let {children: newChildren} = this.getItemsInWindow(this.state.startingIndex, nextProps.children);
    this.setState({
      children: newChildren
    });
  }
  computeHeight() {
    let targetDimensions = document.getElementById(this.props.target).getBoundingClientRect();
    let bodyBottom = document.body.getBoundingClientRect().bottom;
    let targetBottom = targetDimensions.bottom;
    let heightOfList = bodyBottom - targetBottom;
    let numberOfElemsInList = Math.floor(heightOfList / 39);
    let children = this.props.children.slice(0, numberOfElemsInList);
    let numberOfActualElements = children.filter(child => child.props.className.indexOf('column-action-divider') === -1);
    if (children.length > numberOfActualElements.length) {
      children = this.props.children.slice(0, numberOfElemsInList + difference(children.length, numberOfActualElements.length));
    }
    this.setState({
      children,
      numberOfElemsInList,
      startingIndex: 0
    });
  }

  // This is to exclude considering line divider as a child.
  getItemsInWindow(startIndex, children = this.props.children) {
    let nonDividerChildren = children
      .filter(child => child.props.className.indexOf('column-action-divider') === -1);
    nonDividerChildren = nonDividerChildren.slice(startIndex, startIndex + this.state.numberOfElemsInList);
    let actualStartIndex = findIndex(children, nonDividerChildren[0]);
    let actualLastIndex = findIndex(children, nonDividerChildren[nonDividerChildren.length - 1]);
    return {
      children: children.slice(actualStartIndex, actualLastIndex + 1)
    };
  }
  scrollDown() {
    let existingList = this.state.startingIndex + this.state.numberOfElemsInList;
    if ( existingList >= this.props.children.length - 1) {
      return null;
    }
    let startIndex = this.state.startingIndex + 1;
    let {children: newChildren} = this.getItemsInWindow(startIndex);
    this.setState({
      children: newChildren,
      startingIndex: startIndex
    });
  }
  scrollUp() {
    if (this.state.startingIndex === 0) {
      return null;
    }
    let startIndex = this.state.startingIndex - 1;
    let {children: newChildren} = this.getItemsInWindow(startIndex);
    this.setState({
      children: newChildren,
      startingIndex: startIndex
    });
  }
  renderScrollDownContainer() {
    if (this.props.children.length === this.state.startingIndex + this.state.children.length) {
      return null;
    }
    let lastChildInWindow = this.state.children[this.state.children.length - 1].key;
    let lastChild = this.props.children[this.props.children.length - 1].key;
    if (lastChild === lastChildInWindow) {
      return  null;
    }
    return (
      <div
        className="scroll-down-container text-xs-center"
        onClick={this.scrollDown}
      >
        <IconSVG name="icon-caret-down" />
      </div>
    );
  }
  renderScrollUpContainer() {
    if (this.state.startingIndex === 0) {
      return null;
    }
    let firstChildInWindow = this.state.children[0].key;
    let firstChild = this.props.children[0].key;
    if (firstChild === firstChildInWindow) {
      return  null;
    }
    return (
      <div
        className="scroll-down-container text-xs-center"
        onClick={this.scrollUp}
      >
        <IconSVG name="icon-caret-up" />
      </div>
    );
  }
  render() {
    return (
      <div className="scrollable-list">
        {this.renderScrollUpContainer()}
        {this.state.children}
        {this.renderScrollDownContainer()}
      </div>
    );
  }
}
ScrollableList.propTypes = {
  children: PropTypes.arrayOf(PropTypes.node).isRequired,
  target: PropTypes.string.isRequired
};
