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

import React, {PropTypes, Component} from 'react';
import NamespaceStore from 'services/NamespaceStore';
import {Dropdown, DropdownToggle, DropdownItem} from 'reactstrap';
import CustomDropdownMenu from 'components/CustomDropdownMenu';
import {Link} from 'react-router';
import RedirectToLogin from 'services/redirect-to-login';
import cookie from 'react-cookie';
import VersionStore from 'services/VersionStore';
import AboutPageModal from 'components/Header/ProductDropdown/AboutPageModal';
import AccessTokenModal from 'components/Header/ProductDropdown/AccessTokenModal';
import T from 'i18n-react';

require('./ProductDropdown.scss');

export default class ProductDropdown extends Component {
  constructor(props) {
    super(props);
    this.state = {
      toggleDropdown: false,
      aboutPageOpen: false,
      accessTokenModalOpen: false,
      username: NamespaceStore.getState().username
    };
    this.logout = this.logout.bind(this);
    this.toggleCdapMenuDropdown = this.toggleCdapMenuDropdown.bind(this);
    this.toggleAboutPage = this.toggleAboutPage.bind(this);
    this.toggleAccessTokenModal = this.toggleAccessTokenModal.bind(this);
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
    let administrationURL = `${baseCDAPURL}/administration`;
    let dataprepUrl = `${baseCDAPURL}/dataprep`;
    let currentNamespace = NamespaceStore.getState().selectedNamespace;
    let oldUIUrl = `/oldcdap/ns/${currentNamespace}`;
    let userSection;
    if (this.state.username && window.CDAP_CONFIG.securityEnabled) {
      userSection = (
        <ul className="user-profile clearfix">
          <DropdownItem
            tag="li"
            header
          >
            <a className="user-section">
              <span className="fa fa-user"></span>
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
            <div className="cdap-logo">
              <img src="/cdap_assets/img/cdap_logo.png" />
            </div>
            <span className="fa fa-caret-down"></span>
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
                  <Link to={`/administration`}>
                    {T.translate('features.Administration.Title')}
                  </Link>
                :
                  <a href={administrationURL}>
                    {T.translate('features.Administration.Title')}
                  </a>
              }
            </DropdownItem>
            <DropdownItem tag="li">
              <a href={oldUIUrl}>
                {T.translate('features.Navbar.ProductDropdown.olduilink')}
              </a>
            </DropdownItem>
            <DropdownItem tag="li">
              {
                !this.props.nativeLink ?
                  <Link to={`/dataprep`}>
                    {T.translate('features.Navbar.ProductDropdown.dataPrep')}
                    <span className="beta-badge">BETA</span>
                  </Link>
                :
                  <a href={dataprepUrl}>
                    {T.translate('features.Navbar.ProductDropdown.dataPrep')}
                    <span className="beta-badge">BETA</span>
                  </a>
              }
            </DropdownItem>
            <DropdownItem divider />
            <DropdownItem tag="li">
              <a
                target="_blank"
                href="http://cask.co/products/cdap/"
              >
                {T.translate('features.Navbar.ProductDropdown.prodWebsiteLabel')}
              </a>
            </DropdownItem>
            <DropdownItem tag="li">
              <a
                target="_blank"
                href="http://cask.co/community"
              >
                {T.translate('features.Navbar.ProductDropdown.supportLabel')}
              </a>
            </DropdownItem>
            <DropdownItem tag="li">
              <a
                href={docsUrl}
                target="_blank"
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
