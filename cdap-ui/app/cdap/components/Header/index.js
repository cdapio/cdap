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
import T from 'i18n-react';
import NamespaceStore from 'services/NamespaceStore';
import NamespaceDropdown from 'components/NamespaceDropdown';
import ProductDropdown from 'components/Header/ProductDropdown';
import CaskMarketButton from 'components/Header/CaskMarketButton';
import {MyNamespaceApi} from 'api/namespace';
import NamespaceActions from 'services/NamespaceStore/NamespaceActions';
import classnames from 'classnames';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';
import getLastSelectedNamespace from 'services/get-last-selected-namespace';
import NavLinkWrapper from 'components/NavLinkWrapper';
import ControlCenterDropdown from 'components/Header/ControlCenterDropdown';
import {objectQuery} from 'services/helpers';
<<<<<<< HEAD
import {SYSTEM_NAMESPACE} from 'services/global-constants';
=======
import {
  hideDataPrep,
  hidePipelines,
  hideAnalytics,
  hideRulesEngine,
  hideMetadata,
  hideHub,
  getLogo
} from 'services/ThemeHelper';
>>>>>>> 75078e7b6f... Gets customizations from one source (ThemeHelper), instead of having them all over the place

require('./Header.scss');

export default class Header extends Component {
  constructor(props) {
    super(props);
    this.state = {
      toggleNavbar: false,
      currentNamespace: NamespaceStore.getState().selectedNamespace,
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

  isRulesEnginedActive = (match, location = window.location) => {
    if (match && match.isExact) {
      return true;
    }
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    let rulesenginepath = `/cdap/ns/${namespace}/rulesengine`;
    return location.pathname.startsWith(rulesenginepath);
  };

  isDataPrepActive = (match, location = window.location) => {
    if (match && match.isExact) {
      return true;
    }
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    let dataprepBasePath = `/cdap/ns/${namespace}/dataprep`;
    let connectionsBasePath = `/cdap/ns/${namespace}/connections`;

    if (location.pathname.startsWith(dataprepBasePath) || location.pathname.startsWith(connectionsBasePath)) {
      return true;
    }
    return false;
  };

  isMMDSActive = (match, location = window.location) => {
    if (match && match.isExact) {
      return true;
    }
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    let experimentsBasePath = `/cdap/ns/${namespace}/experiments`;
    return location.pathname.startsWith(experimentsBasePath);
  };

  isCDAPActive = () => {
    let location = window.location;
    let {selectedNamespace: namespace} = NamespaceStore.getState();

    let basePath = `/cdap/ns/${namespace}`;

    let dataprepBasePath = `${basePath}/dataprep`;
    let connectionsBasePath = `${basePath}/connections`;
    let rulesenginepath = `${basePath}/rulesengine`;
    let analytics = `${basePath}/experiments`;
    let namespaceDetails = `${basePath}/details`;
    let createProfile = `${basePath}/profiles/create`;
    let profileDetails = `${basePath}/profiles/details`;

    if (
      location.pathname.startsWith(basePath) &&
      !location.pathname.startsWith(dataprepBasePath) &&
      !location.pathname.startsWith(connectionsBasePath) &&
      !location.pathname.startsWith(rulesenginepath) &&
      !location.pathname.startsWith(analytics) &&
      !location.pathname.startsWith(namespaceDetails) &&
      !location.pathname.startsWith(createProfile) &&
      !location.pathname.startsWith(profileDetails)
    ) {
      return true;
    }
    return false;
  };

  renderBrandSection() {
    const baseCDAPUrl = `/ns/${this.state.currentNamespace}`;
    const brandLogoSrc = getLogo() || '/cdap_assets/img/company_logo.png';
    return (
      <div className="brand-section">
          <NavLinkWrapper
            isNativeLink={this.props.nativeLink}
            to={this.props.nativeLink ? `/cdap${baseCDAPUrl}` : baseCDAPUrl}
          >
            <img src={brandLogoSrc} />
          </NavLinkWrapper>
        </div>
    );
  }

  renderControlCenterDropdown() {
    return (
      <li className={classnames({ 'active': this.isCDAPActive() })}>
        <ControlCenterDropdown
          nativeLink={this.props.nativeLink}
          namespace={this.state.currentNamespace}
        />
      </li>
    );
  }

  renderDataPrepLink() {
    if (hideDataPrep()) {
      return null;
    }

    const dataPrepUrl = `/ns/${this.state.currentNamespace}/dataprep`;
    return (
      <li className={classnames({
        'active': this.isDataPrepActive()
      })}>
        <NavLinkWrapper
          isNativeLink={this.props.nativeLink}
          to={this.props.nativeLink ? `/cdap${dataPrepUrl}` : dataPrepUrl}
        >
          {T.translate(`features.Navbar.Dataprep`)}
        </NavLinkWrapper>
      </li>
    );
  }

  renderPipelinesLink() {
    if (hidePipelines()) {
      return null;
    }

    const pipelinesListUrl =  window.getHydratorUrl({
      stateName: 'hydrator.list',
      stateParams: {
        namespace: this.state.currentNamespace,
        page: 1,
        sortBy: '_stats.lastStartTime'
      }
    });
    const isPipelinesViewActive = location.pathname.indexOf('/pipelines/') !== -1;

    return (
      <li className={classnames({
        'active': isPipelinesViewActive
      })}>
        <a href={pipelinesListUrl}>
          {T.translate('features.Navbar.pipelinesLabel')}
        </a>
      </li>
    );
  }

  renderRulesEngineLink() {
    if (hideRulesEngine()) {
      return null;
    }

    const rulesEngineUrl = `/ns/${this.state.currentNamespace}/rulesengine`;
    return (
      <li className={classnames({
        'active': this.isRulesEnginedActive()
      })}>
          <NavLinkWrapper
            isNativeLink={this.props.nativeLink}
            to={this.props.nativeLink ? `/cdap${rulesEngineUrl}` : rulesEngineUrl}
          >
            {T.translate(`features.Navbar.rulesmgmt`)}
          </NavLinkWrapper>
      </li>
    );
  }

  renderAnalyticsLink() {
    if (hideAnalytics()) {
      return null;
    }

    const analyticsUrl = `/ns/${this.state.currentNamespace}/experiments`;
    return (
      <li className={classnames({
        'active': this.isMMDSActive()
      })}>
        <NavLinkWrapper
          isNativeLink={this.props.nativeLink}
          to={this.props.nativeLink ? `/cdap${analyticsUrl}` : analyticsUrl}
          >
          {T.translate(`features.Navbar.MMDS`)}
        </NavLinkWrapper>
      </li>
    );
  }

  renderMetadataLink() {
    if (hideMetadata()) {
      return null;
    }

    const isMetadataActive = location.pathname.indexOf('metadata') !== -1;
    return (
      <li className={classnames({'active': isMetadataActive})}>
        <MetadataDropdown />
      </li>
    );
  }

  renderHubButton() {
    if (hideHub()) {
      return null;
    }

    return (
      <li className="with-pointer">
        <CaskMarketButton>
          <span className="hub-text-wrapper">{T.translate('commons.market')}</span>
        </CaskMarketButton>
      </li>
    );
  }

  render() {
    let administrationURL = '/administration/configuration';

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
        {this.renderBrandSection()}
        <ul className="navbar-list-section control-center">
          {this.renderControlCenterDropdown()}
          {this.renderDataPrepLink()}
          {this.renderPipelinesLink()}
          {this.renderAnalyticsLink()}
          {this.renderRulesEngineLink()}
          {this.renderMetadataLink()}
        </ul>
        <div className={classnames("global-navbar-collapse", {
            'minimized': this.state.toggleNavbar
          })}>
          <div className="navbar-right-section">
            <ul>
              {this.renderHubButton()}
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
