/*
 * Copyright © 2019 Cask Data, Inc.
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

import React from 'react';
import MenuItem from '@material-ui/core/MenuItem';
import T from 'i18n-react';
import If from 'components/If';
import { ClickAwayListener } from '@material-ui/core';
import Grow from '@material-ui/core/Grow';
import Paper from '@material-ui/core/Paper';
import Popper from '@material-ui/core/Popper';
import VersionStore from 'services/VersionStore';
import AboutPageModal from 'components/AppHeader/AboutPageModal';
import { Theme } from 'services/ThemeHelper';
import IconSVG from 'components/IconSVG';
import classnames from 'classnames';
import IconButton from '@material-ui/core/IconButton';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import NamespaceStore from 'services/NamespaceStore';
import Divider from '@material-ui/core/Divider';
import AccessTokenModal from 'components/AppHeader/AccessTokenModal';
import cookie from 'react-cookie';
import RedirectToLogin from 'services/redirect-to-login';

interface IAppToolbarState {
  anchorEl: EventTarget | null;
  aboutPageOpen: boolean;
  accessTokenModalOpen: boolean;
}
const styles = (theme) => ({
  root: {
    width: '200px',
  },
  buttonLink: theme.buttonLink,
  cogWheelFontSize: {
    // This is because the icon is not the same size as it should be.
    // So beside a normal text this looks small. Hence the bump in font size
    fontSize: '1.3rem',
  },
  anchorMenuItem: {
    fontSize: '1rem',
    '&:focus': {
      outline: 'none',
    },
    textDecoration: 'none !important',
  },
  iconButtonFocus: theme.iconButtonFocus,
  usernameStyles: {
    padding: '0 4px',
  },
  linkStyles: {
    color: 'inherit' as 'inherit',
  },
});
interface IAppToolbarMenuProps extends WithStyles<typeof styles> {}

class AppToolbarMenu extends React.Component<IAppToolbarMenuProps, IAppToolbarState> {
  public state = {
    anchorEl: null,
    aboutPageOpen: false,
    username: NamespaceStore.getState().username,
    accessTokenModalOpen: false,
  };

  private toggleAboutPage = () => {
    this.setState({
      aboutPageOpen: !this.state.aboutPageOpen,
    });
  };

  private toggleAccessTokenModal = () => {
    this.setState({
      accessTokenModalOpen: !this.state.accessTokenModalOpen,
    });
  };
  private getDocsUrl = () => {
    if (Theme.productDocumentationLink === null) {
      const cdapVersion = VersionStore.getState().version;
      return `http://docs.cdap.io/cdap/${cdapVersion}/en/index.html`;
    }

    return Theme.productDocumentationLink;
  };
  public toggleSettings = (event: React.MouseEvent<HTMLElement>) => {
    if (this.state.anchorEl) {
      this.setState({
        anchorEl: null,
      });
    } else {
      this.setState({
        anchorEl: event.currentTarget,
      });
    }
  };

  public closeSettings = (e) => {
    if (this.state.anchorEl && this.state.anchorEl.contains(e.target)) {
      return;
    }
    this.setState({
      anchorEl: null,
    });
  };

  private onLogout() {
    cookie.remove('show-splash-screen-for-session', { path: '/' });
    RedirectToLogin({ statusCode: 401 });
  }

  public render() {
    const { anchorEl } = this.state;
    const { classes } = this.props;
    const cdapVersion = VersionStore.getState().version;
    return (
      <React.Fragment>
        <div onClick={this.toggleSettings}>
          <IconButton className={classnames(classes.buttonLink, classes.iconButtonFocus)}>
            <IconSVG name="icon-cogs" className={classes.cogWheelFontSize} />
          </IconButton>
        </div>
        <Popper
          open={Boolean(anchorEl)}
          anchorEl={anchorEl}
          transition
          disablePortal
          className={classes.root}
        >
          {({ TransitionProps, placement }) => (
            <Grow
              {...TransitionProps}
              style={{ transformOrigin: placement === 'bottom' ? 'center top' : 'center bottom' }}
            >
              <Paper>
                <ClickAwayListener onClickAway={this.closeSettings}>
                  <a
                    className={classes.linkStyles}
                    href={this.getDocsUrl()}
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    <MenuItem onClick={this.closeSettings} className={classes.anchorMenuItem}>
                      {T.translate('features.Navbar.ProductDropdown.documentationLabel')}
                    </MenuItem>
                  </a>
                  <If condition={Theme.showAboutProductModal === true}>
                    <MenuItem onClick={this.toggleAboutPage} className={classes.anchorMenuItem}>
                      <a className={classes.linkStyles}>
                        {T.translate('features.Navbar.ProductDropdown.aboutLabel', {
                          productName: Theme.productName,
                        })}
                      </a>
                    </MenuItem>
                  </If>
                  <If condition={this.state.username && window.CDAP_CONFIG.securityEnabled}>
                    <React.Fragment>
                      <Divider />
                      <MenuItem className={classes.anchorMenuItem}>
                        <IconSVG name="icon-user" />
                        <a className={`${classes.usernameStyles} ${classes.linkStyles}`}>
                          {this.state.username}
                        </a>
                      </MenuItem>
                      <MenuItem
                        onClick={this.toggleAccessTokenModal}
                        className={classes.anchorMenuItem}
                      >
                        <a className={classes.linkStyles}>
                          {T.translate('features.Navbar.ProductDropdown.accessToken')}{' '}
                        </a>
                      </MenuItem>
                      <MenuItem onClick={this.onLogout} className={classes.anchorMenuItem}>
                        <a className={classes.linkStyles}>
                          {T.translate('features.Navbar.ProductDropdown.logout')}
                        </a>
                      </MenuItem>
                    </React.Fragment>
                  </If>
                </ClickAwayListener>
              </Paper>
            </Grow>
          )}
        </Popper>
        <If condition={this.state.username && window.CDAP_CONFIG.securityEnabled}>
          <AccessTokenModal
            cdapVersion={cdapVersion}
            isOpen={this.state.accessTokenModalOpen}
            toggle={this.toggleAccessTokenModal}
          />
        </If>
        <If condition={Theme.showAboutProductModal === true}>
          <AboutPageModal
            cdapVersion={cdapVersion}
            isOpen={this.state.aboutPageOpen}
            toggle={this.toggleAboutPage}
          />
        </If>
      </React.Fragment>
    );
  }
}

export default withStyles(styles)(AppToolbarMenu);
