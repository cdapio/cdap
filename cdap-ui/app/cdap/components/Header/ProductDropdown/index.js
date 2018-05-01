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
import NamespaceStore from 'services/NamespaceStore';
import {Dropdown, DropdownToggle, DropdownItem} from 'reactstrap';
import CustomDropdownMenu from 'components/CustomDropdownMenu';
import {Link} from 'react-router-dom';
import RedirectToLogin from 'services/redirect-to-login';
import cookie from 'react-cookie';
import VersionStore from 'services/VersionStore';
import AboutPageModal from 'components/Header/ProductDropdown/AboutPageModal';
import AccessTokenModal from 'components/Header/ProductDropdown/AccessTokenModal';
import IconSVG from 'components/IconSVG';
import getLastSelectedNamespace from 'services/get-last-selected-namespace';
import T from 'i18n-react';
import {getMode} from 'components/Header/ProductDropdown/helper';

require('./ProductDropdown.scss');

export default class ProductDropdown extends Component {
  constructor(props) {
    super(props);
    this.state = {
      toggleDropdown: false,
      aboutPageOpen: false,
      accessTokenModalOpen: false,
      username: NamespaceStore.getState().username,
      currentNamespace: null
    };
    this.logout = this.logout.bind(this);
    this.toggleCdapMenuDropdown = this.toggleCdapMenuDropdown.bind(this);
    this.toggleAboutPage = this.toggleAboutPage.bind(this);
    this.toggleAccessTokenModal = this.toggleAccessTokenModal.bind(this);
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

  toggleCdapMenuDropdown() {
    this.setState({
      toggleDropdown: !this.state.toggleDropdown
    });
  }

  toggleAboutPage() {
    this.setState({
      aboutPageOpen: !this.state.aboutPageOpen
    });
  }

  toggleAccessTokenModal() {
    this.setState({
      accessTokenModalOpen: !this.state.accessTokenModalOpen
    });
  }

  logout() {
    cookie.remove('show-splash-screen-for-session', {path: '/'});
    RedirectToLogin({statusCode: 401});
  }

  onProfileClick(e) {
    e.nativeEvent.preventDefault();
    e.nativeEvent.stopPropagation();
    e.nativeEvent.stopImmediatePropagation();
    return false;
  }

  render() {
    let baseCDAPURL = window.getAbsUIUrl();
    let cdapVersion = VersionStore.getState().version;
    let docsUrl = `http://docs.cask.co/cdap/${cdapVersion}/en/index.html`;
    let administrationURL = '/administration/configuration';
    let oldUIUrl = `/oldcdap/ns/${this.state.currentNamespace}`;
    let mode = getMode();
    let userSection;
    if (this.state.username && window.CDAP_CONFIG.securityEnabled) {
      userSection = (
        <ul className="user-profile clearfix">
          <DropdownItem
            tag="li"
            header
          >
            <a className="user-section">
              <span className="user-icon-container">
                <IconSVG name="icon-user" />
              </span>
              <span className="user-name">{this.state.username}</span>
            </a>
          </DropdownItem>
          <DropdownItem
            tag="li"
            onClick={this.toggleAccessTokenModal}
          >
            <a>{T.translate('features.Navbar.ProductDropdown.accessToken')}</a>
          </DropdownItem>
          <DropdownItem
            tag="li"
            onClick={this.logout}
          >
            <a>{T.translate('features.Navbar.ProductDropdown.logout')}</a>
          </DropdownItem>
          <AccessTokenModal
            cdapVersion={cdapVersion}
            isOpen={this.state.accessTokenModalOpen}
            toggle={this.toggleAccessTokenModal}
          />
        </ul>
      );
    }

    return (
      <div>
        <Dropdown
          isOpen={this.state.toggleDropdown}
          className="product-dropdown"
          toggle={this.toggleCdapMenuDropdown.bind(this)}>
          <DropdownToggle caret>
            <div className="cdap-logo-container">
              <div className="cdap-logo">
                <img src="/cdap_assets/img/cdap_logo.png" />
              </div>
              <div className="caret-down-container">
                <IconSVG name="icon-caret-down" />
              </div>
            </div>
            <div className="secure-mode-icon">
              <div className="cdap-mode">{mode}</div>
              <IconSVG name={ window.CDAP_CONFIG.securityEnabled ? "icon-lock_close" : "" } />
            </div>
          </DropdownToggle>
          <CustomDropdownMenu right>
            <DropdownItem
              tag="li"
              onClick={this.toggleAboutPage}
            >
              <a>{T.translate('features.Navbar.ProductDropdown.aboutLabel')}</a>
            </DropdownItem>
            <DropdownItem divider />
            <DropdownItem tag="li">
              {
                !this.props.nativeLink ?
                  <Link to={administrationURL}>
                    {T.translate('features.Administration.Title')}
                  </Link>
                :
                  <a href={`${baseCDAPURL}${administrationURL}`}>
                    {T.translate('features.Administration.Title')}
                  </a>
              }
            </DropdownItem>
            <DropdownItem tag="li">
              <a href={oldUIUrl}>
                {T.translate('features.Navbar.ProductDropdown.olduilink')}
              </a>
            </DropdownItem>
            <DropdownItem divider />
            <DropdownItem tag="li">
              <a
                target="_blank"
                rel="noopener noreferrer"
                href="http://cask.co/products/cdap/"
              >
                {T.translate('features.Navbar.ProductDropdown.prodWebsiteLabel')}
              </a>
            </DropdownItem>
            <DropdownItem tag="li">
              <a
                target="_blank"
                rel="noopener noreferrer"
                href="http://cask.co/community"
              >
                {T.translate('features.Navbar.ProductDropdown.supportLabel')}
              </a>
            </DropdownItem>
            <DropdownItem tag="li">
              <a
                href={docsUrl}
                target="_blank"
                rel="noopener noreferrer"
              >
                {T.translate('features.Navbar.ProductDropdown.documentationLabel')}
              </a>
            </DropdownItem>
            {
              window.CDAP_CONFIG.securityEnabled ?
                (
                  <DropdownItem divider />
                )
              :
                null
            }
            {
              window.CDAP_CONFIG.securityEnabled ?
                userSection
              :
                null
            }
          </CustomDropdownMenu>
        </Dropdown>
        <AboutPageModal
          cdapVersion={cdapVersion}
          isOpen={this.state.aboutPageOpen}
          toggle={this.toggleAboutPage}
        />
      </div>
    );
  }
}
ProductDropdown.propTypes = {
  nativeLink: PropTypes.bool
};
