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
import { ButtonDropdown, DropdownToggle, DropdownMenu, DropdownItem } from 'reactstrap';
import T from 'i18n-react';
import classnames from 'classnames';
import NamespaceStore from 'services/NamespaceStore';
import Rx from 'rx';

require('./JumpButton.less');

export default class JumpButton extends Component {
  constructor(props) {
    super(props);

    this.toggle = this.toggle.bind(this);
    this.viewInTrackerLink = this.viewInTrackerLink.bind(this);

    this.state = {
      dropdownOpen: false
    };

    this.getJumpActions();
    this.documentClickEventListener$ = Rx.Observable.fromEvent(document, 'click')
      .subscribe(() => {
        if (this.state.dropdownOpen) {
          this.setState({
            dropdownOpen: !this.state.dropdownOpen
          });
        }
      });
  }

  componentWillUnmount() {
    this.documentClickEventListener$.dispose();
  }

  toggle(event) {
    this.setState({
      dropdownOpen: !this.state.dropdownOpen
    });
    event.stopPropagation();
    event.nativeEvent.stopImmediatePropagation();
  }

  viewInTrackerLink() {
    const entity = this.props.entity;
    const entityTypeConvert = {
      'stream': 'streams',
      'datasetinstance': 'datasets'
    };

    const stateName = 'tracker.detail.entity.metadata';
    let stateParams = {
      namespace: NamespaceStore.getState().selectedNamespace,
      entityType: entityTypeConvert[entity.type],
      entityId: entity.id
    };

    return window.getTrackerUrl({ stateName, stateParams});
  }

  viewInHydratorLink() {
    const entity = this.props.entity;
    const stateName = 'hydrator.detail';

    let stateParams = {
      namespace: NamespaceStore.getState().selectedNamespace,
      pipelineId: entity.id
    };

    return window.getHydratorUrl({stateName, stateParams});
  }

  getStreamAndDatasetJumpActions() {
    return (
      <DropdownItem
        tag="a"
        href={this.viewInTrackerLink()}
      >
        <span className="icon-tracker action-icon"></span>
        <span>{T.translate('features.JumpButton.viewTracker')}</span>
      </DropdownItem>
    );
  }

  getHydratorAction() {
    return (
      <DropdownItem
        tag="a"
        href={this.viewInHydratorLink()}
      >
        <span className="icon-hydrator action-icon"></span>
        <span>{T.translate('features.JumpButton.viewHydrator')}</span>
      </DropdownItem>
    );
  }

  getJumpActions() {
    const entity = this.props.entity;
    switch (entity.type) {
      case 'stream':
      case 'datasetinstance':
        return this.getStreamAndDatasetJumpActions();
      case 'application':
        return this.getHydratorAction();
    }
  }

  render() {
    return (
      <ButtonDropdown isOpen={this.state.dropdownOpen} toggle={() => {}}>
        <DropdownToggle className="jump-button clearfix" onClick={this.toggle}>
          <span className="pull-left">{T.translate('features.JumpButton.buttonLabel')}</span>
          <span className={classnames('fa pull-right', {
            'fa-chevron-down': !this.state.dropdownOpen,
            'fa-chevron-up': this.state.dropdownOpen
          })} />
        </DropdownToggle>
        <DropdownMenu className="jump-button-dropdown" right>
          {this.getJumpActions()}
        </DropdownMenu>
      </ButtonDropdown>
    );
  }
}

JumpButton.propTypes = {
  entity: PropTypes.object
};
