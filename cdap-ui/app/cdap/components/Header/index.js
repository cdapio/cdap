/*
 * Copyright © 2017-2018 Cask Data, Inc.
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
import NamespaceStore from 'services/NamespaceStore';
import NamespaceDropdown from 'components/NamespaceDropdown';
import ProductDropdown from 'components/Header/ProductDropdown';
import {MyNamespaceApi} from 'api/namespace';
import NamespaceActions from 'services/NamespaceStore/NamespaceActions';
import classnames from 'classnames';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';
import getLastSelectedNamespace from 'services/get-last-selected-namespace';
import ControlCenterDropdown from 'components/Header/ControlCenterDropdown';
import {objectQuery} from 'services/helpers';
import {SYSTEM_NAMESPACE} from 'services/global-constants';
import BrandSection from 'components/Header/BrandSection';
import DataPrepLink from 'components/Header/DataPrepLink';
import PipelinesLink from 'components/Header/PipelinesLink';
import AnalyticsLink from 'components/Header/AnalyticsLink';
import RulesEngineLink from 'components/Header/RulesEngineLink';
import MetadataLink from 'components/Header/MetadataLink';
import HubButton from 'components/Header/HubButton';
import {NamespaceLinkContext} from 'components/Header/NamespaceLinkContext';
import Welcome from 'components/Header/Welcome';

require('./Header.scss');

export default class Header extends Component {
  constructor(props) {
    super(props);
    this.state = {
      toggleNavbar: false,
      currentNamespace: NamespaceStore.getState().selectedNamespace
    };
    this.namespacesubscription = null;
    this.eventEmitter = ee(ee);
  }
  componentWillMount() {
    // Polls for namespace data
    this.namespacesubscription = MyNamespaceApi.pollList()
      .subscribe(
        (res) => {
          if (res.length > 0) {
            NamespaceStore.dispatch({
              type: NamespaceActions.updateNamespaces,
              payload: {
                namespaces : res
              }
            });
          } else {
            // TL;DR - This is emitted for Authorization in main.js
            // This means there is no namespace for the user to work on.
            // which indicates she/he have no authorization for any namesapce in the system.
            this.eventEmitter.emit(globalEvents.NONAMESPACE);
          }
        }
      );
    this.nsSubscription = NamespaceStore.subscribe(() => {
      let selectedNamespace = getLastSelectedNamespace();
      let {namespaces} = NamespaceStore.getState();
      if (selectedNamespace === SYSTEM_NAMESPACE) {
        selectedNamespace = objectQuery(namespaces, 0, 'name');
      }
      if (selectedNamespace !== this.state.currentNamespace) {
        this.setState({
          currentNamespace: selectedNamespace
        });
      }
    });
  }
  componentWillUnmount() {
    this.nsSubscription();
    if (this.namespacesubscription) {
      this.namespacesubscription.unsubscribe();
    }
  }
  toggleNavbar() {
    this.setState({
      toggleNavbar: !this.state.toggleNavbar
    });
  }

  render() {
    let administrationURL = '/administration/configuration';
    const namespaceLinkContext = {
      namespace: this.state.currentNamespace,
      isNativeLink: this.props.nativeLink
    };
    return (
      <div className="global-navbar">
        <div
          className="global-navbar-toggler float-xs-right btn"
          onClick={this.toggleNavbar.bind(this)}
        >
          {
            !this.state.toggleNavbar ?
              <i className="fa fa-bars fa-2x"></i>
            :
              <i className="fa fa-times fa-2x"></i>
          }
        </div>
        <NamespaceLinkContext.Provider value={namespaceLinkContext}>
          <BrandSection />
          <ul className="navbar-list-section control-center">
            <ControlCenterDropdown
              nativeLink={this.props.nativeLink}
              namespace={this.state.currentNamespace}
            />
            <DataPrepLink />
            <PipelinesLink />
            <AnalyticsLink />
            <RulesEngineLink />
            <MetadataLink />
          </ul>
        </NamespaceLinkContext.Provider>
        <div className={classnames("global-navbar-collapse", {
            'minimized': this.state.toggleNavbar
          })}>
          <div className="navbar-right-section">
            <ul>
              <HubButton />
              <li
                id="header-namespace-dropdown"
                className="with-pointer namespace-dropdown-holder">
                {
                  !this.props.nativeLink ?
                    <NamespaceDropdown />
                  :
                    <NamespaceDropdown tag="a"/>
                }
              </li>
              <li className={classnames("with-pointer cdap-menu clearfix", {
                'admin-view': administrationURL === location.pathname.replace(/\/cdap/, '')
              })}>
                <ProductDropdown
                  nativeLink={this.props.nativeLink}
                />
              </li>
            </ul>
          </div>
        </div>
        <Welcome />
      </div>
    );
  }
}

Header.defaultProps = {
  nativeLink: false
};

Header.propTypes = {
  nativeLink: PropTypes.bool
};
