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
import React, {Component} from 'react';
import {Dropdown, DropdownToggle, DropdownItem} from 'reactstrap';
import CustomDropdownMenu from 'components/CustomDropdownMenu';
import NamespaceStore from 'services/NamespaceStore';
import T from 'i18n-react';
import classnames from 'classnames';
import getLastSelectedNamespace from 'services/get-last-selected-namespace';

require('./MetadataDropdown.scss');

export default class MetadataDropdown extends Component {
  constructor(props) {
    super(props);
    this.state = {
      toggleDropdown: false,
      currentNamespace: null
    };
  }
  toggleMetadataDropdown() {
    this.setState({
      toggleDropdown: !this.state.toggleDropdown
    });
  }

  componentWillMount() {
    this.nsSubscription = NamespaceStore.subscribe(() => {
      let selectedNamespace = getLastSelectedNamespace();
      if (selectedNamespace !== this.state.currentNamespace) {
        this.setState({
          currentNamespace: selectedNamespace
        });
      }
    });
  }
  componentWillUnmount() {
    this.nsSubscription();
  }
  render() {
    let searchHomeUrl = window.getTrackerUrl({
        stateName: 'tracker',
        stateParams: {
          namespace: this.state.currentNamespace
        }
    });
    let tagsHomeUrl = window.getTrackerUrl({
        stateName: 'tracker.tags',
        stateParams: {
          namespace: this.state.currentNamespace
        }
    });
    let dictionaryHomeUrl = window.getTrackerUrl({
        stateName: 'tracker.dictionary',
        stateParams: {
          namespace: this.state.currentNamespace
        }
    });
    let integrationsHomeUrl = window.getTrackerUrl({
        stateName: 'tracker.integrations',
        stateParams: {
          namespace: this.state.currentNamespace
        }
    });
    return (
      <Dropdown
        className={classnames("metadata-dropdown", {'active': location.pathname.indexOf('metadata') !== -1})}
        isOpen={this.state.toggleDropdown}
        toggle={this.toggleMetadataDropdown.bind(this)}
      >
        <DropdownToggle
          caret
          className={classnames({'active': location.pathname.indexOf('metadata') !== -1})}
        >
          {T.translate('features.Navbar.metadataLabel')}
        </DropdownToggle>
        <CustomDropdownMenu>
          <DropdownItem tag="li">
            {/* FIXME: Error prone. This active class check is to see if its a sub path of search.*/}
            <a
              className={classnames({'active': searchHomeUrl === location.href || location.href.indexOf('search') !== -1 })}
              href={searchHomeUrl}>
              {T.translate('features.Navbar.Metadata.searchLabel')}
            </a>
          </DropdownItem>
          <DropdownItem tag="li">
            <a
              className={classnames({'active': tagsHomeUrl === location.href})}
              href={tagsHomeUrl}>
              {T.translate('features.Navbar.Metadata.tagsLabel')}
            </a>
          </DropdownItem>
          <DropdownItem tag="li">
            <a
              className={classnames({'active': location.href === dictionaryHomeUrl})}
              href={dictionaryHomeUrl}>
              {T.translate('features.Navbar.Metadata.dictionaryLabel')}
            </a>
          </DropdownItem>
          <DropdownItem tag="ul" divider />
          <DropdownItem tag="li">
            <a
              className={classnames({'active': integrationsHomeUrl === location.href})}
              href={integrationsHomeUrl}
            >
              {T.translate('features.Navbar.Metadata.integrationsLabel')}
            </a>
          </DropdownItem>
        </CustomDropdownMenu>
      </Dropdown>
    );
  }
}
