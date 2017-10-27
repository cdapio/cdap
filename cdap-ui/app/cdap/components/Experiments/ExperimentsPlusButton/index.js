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
import Rx from 'rx';
import shortid from 'shortid';
import {Link} from 'react-router-dom';
import NamespaceStore from 'services/NamespaceStore';

require('./ExperimentsPlusButton.scss');

export default class ExperimentsPlusButton extends Component {
  constructor(props) {
    super(props);
    this.state = {
      dropdownOpen: props.dropdownOpen,
      id: shortid.generate()
    };
    this.togglePopover = this.togglePopover.bind(this);
    this.itemClicked = this.itemClicked.bind(this);
  }
  componentWillUnmount() {
    if (this.documentClick$) {
      this.documentClick$.dispose();
    }
  }
  togglePopover() {
    let newState = !this.state.dropdownOpen;
    this.setState({
      dropdownOpen: newState
    });

    if (this.props.documentElement && newState) {
      this.documentClick$ = Rx.Observable.fromEvent(this.props.documentElement, 'click')
      .subscribe((e) => {
        if (isDescendant(this.popover, e.target) || !this.state.dropdownOpen) {
          return;
        }

        this.togglePopover();
      });
      Mousetrap.bind('esc', this.togglePopover);
    } else {
      if (this.documentClick$) {
        this.documentClick$.dispose();
      }
      Mousetrap.unbind('esc');
    }
  }
  itemClicked() {
    this.togglePopover();
  }
  renderPopover() {
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    return (
      <Popover
        toggle={this.togglePopover}
        placement="top right"
        isOpen={this.state.dropdownOpen}
        target={this.state.id}
        className="add-experiment-dropdown"
        tether={{classPrefix: 'add-experiment-popover'}}
      >
        <PopoverContent onClick={this.itemClicked}>
          <Link to={`/ns/${namespace}/experiments/create`}>
            Create a new Experiment
          </Link>
          <hr />
          <div> More </div>
        </PopoverContent>
      </Popover>
    );
  }
  render() {
    return (
      <span
        className="experiments-plus-btn"
        id={this.state.id}
        onClick={this.togglePopover}
        ref={(ref) => this.popover = ref}
      >
        <img
          className="button-container"
          src="/cdap_assets/img/plus_ico.svg"
        />
        {this.renderPopover()}
      </span>
    );
  }
}
ExperimentsPlusButton.propTypes = {
  children: PropTypes.node.isRequired,
  dropdownOpen: PropTypes.bool,
  tetherOption: PropTypes.object,
  documentElement: PropTypes.node,
  icon: PropTypes.string
};
