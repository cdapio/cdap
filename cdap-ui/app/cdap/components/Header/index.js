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

import React, {Component, PropTypes} from 'react';
import {Link} from 'react-router';
import T from 'i18n-react';
import NamespaceStore from 'services/NamespaceStore';
import NamespaceDropdown from 'components/NamespaceDropdown';
import ProductDropdown from 'components/Header/ProductDropdown';
import MetadataDropdown from 'components/Header/MetadataDropdown';
import CaskMarketButton from 'components/Header/CaskMarketButton';
import {MyNamespaceApi} from 'api/namespace';
import NamespaceActions from 'services/NamespaceStore/NamespaceActions';
import find from 'lodash/find';
import classnames from 'classnames';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';

require('./Header.scss');

export default class Header extends Component {
  constructor(props) {
    super(props);
    this.state = {
      toggleNavbar: false,
      currentNamespace: null,
      metadataDropdown: false
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
      let selectedNamespace = this.getDefaultNamespace();
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
      this.namespacesubscription.dispose();
    }
  }
  findNamespace(list, name) {
    return find(list, {name: name});
  }
  getDefaultNamespace() {
    let list = NamespaceStore.getState().namespaces;
    if (list.length === 0) { return; }
    let selectedNamespace;
    let defaultNamespace = localStorage.getItem('DefaultNamespace');
    let defaultNsFromBackend = list.filter(ns => ns.name === defaultNamespace);
    if (defaultNsFromBackend.length) {
      selectedNamespace = defaultNsFromBackend[0];
    }
    // Check #2
    if (!selectedNamespace) {
      selectedNamespace = this.findNamespace(list, 'default');
    }
    // Check #3
    if (!selectedNamespace) {
      selectedNamespace = list[0].name;
    } else {
      selectedNamespace = selectedNamespace.name;
    }
    return selectedNamespace;
  }
  toggleNavbar() {
    this.setState({
      toggleNavbar: !this.state.toggleNavbar
    });
  }

  render() {
    let baseCDAPURL = window.getAbsUIUrl({
      namespace: this.state.currentNamespace
    });
    let overviewUrl = `${baseCDAPURL}/ns/${this.state.currentNamespace}`;
    let pipelinesListUrl =  window.getHydratorUrl({
      stateName: 'hydrator.list',
      stateParams: {
        namespace: this.state.currentNamespace
      }
    });
    let isPipelinesViewActive = location.pathname.indexOf('/pipelines/') !== -1;
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
        <div className="brand-section">
          {
            !this.props.nativeLink ?
              <Link
                to={`/ns/${this.state.currentNamespace}`}
              >
                <img src="/cdap_assets/img/company_logo.png" />
              </Link>
            :
              <a href={window.getAbsUIUrl({namespaceId: this.state.currentNamespace})}>
                <img src="/cdap_assets/img/company_logo.png" />
              </a>
          }
        </div>
        <ul className="navbar-list-section">
          <li>
            {
              !this.props.nativeLink ?
                <Link
                  activeClassName="active"
                  to={`/ns/${this.state.currentNamespace}`}
                >
                  {T.translate('features.Navbar.overviewLabel')}
                </Link>
              :
                <a href={overviewUrl}>
                  {T.translate('features.Navbar.overviewLabel')}
                </a>
            }
          </li>
          <li>
            <a
              href={pipelinesListUrl}
              className={classnames({'active': isPipelinesViewActive})}
            >
              {T.translate('features.Navbar.pipelinesLabel')}
            </a>
          </li>
          <li>
            <MetadataDropdown />
          </li>
        </ul>
        <div className={classnames("global-navbar-collapse", {
            'minimized': this.state.toggleNavbar
          })}>
          <div className="navbar-right-section">
            <ul>
              <li className="with-shadow">
                <CaskMarketButton>
                  <span className="fa icon-CaskMarket"></span>
                  <span>{T.translate('commons.market')}</span>
                </CaskMarketButton>
              </li>
              <li
                id="header-namespace-dropdown"
                className="with-shadow namespace-dropdown-holder">
                {
                  !this.props.nativeLink ?
                    <NamespaceDropdown />
                  :
                    <NamespaceDropdown tag="a"/>
                }
              </li>
              <li className="with-shadow cdap-menu clearfix">
                <ProductDropdown
                  nativeLink={this.props.nativeLink}
                />
              </li>
            </ul>
          </div>
        </div>
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
