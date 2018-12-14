/*
 * Copyright © 2018 Cask Data, Inc.
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
import * as React from 'react';
import Toolbar from '@material-ui/core/Toolbar';
import IconButton from '@material-ui/core/IconButton';
import BrandImage from 'components/AppHeader/BrandImage';
import Button from '@material-ui/core/Button';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import IconSVG from 'components/IconSVG';
import MenuIcon from '@material-ui/icons/Menu';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import classnames from 'classnames';
import ExtendedLinkButton from 'components/AppHeader/ExtendedLinkButton';
import { withContext, INamespaceLinkContext } from 'components/AppHeader/NamespaceLinkContext';
import ToolBarFeatureLink from 'components/AppHeader/AppToolBar/ToolBarFeatureLink';
import HubButton from 'components/AppHeader/HubButton';
import { Theme } from 'services/ThemeHelper';
import VersionStore from 'services/VersionStore';
import T from 'i18n-react';
import If from 'components/If';
import AboutPageModal from 'components/AppHeader/AboutPageModal';

const styles = (theme) => {
  return {
    grow: theme.grow,
    iconButton: {
      marginLeft: '-20px',
      padding: '10px',
    },
    iconButtonFocus: theme.iconButtonFocus,
    customToolbar: {
      height: '48px',
      minHeight: '48px',
    },
    buttonLink: theme.buttonLink,
    cogWheelFontSize: {
      // This is because the icon is not the same size as it should be.
      // So beside a normal text this looks small. Hence the bump in font size
      fontSize: '1.3rem',
    },
    anchorMenuItem: {
      textDecoration: 'none',
      color: 'inherit',
    },
  };
};

interface IAppToolbarProps extends WithStyles<typeof styles> {
  onMenuIconClick: () => void;
  context: INamespaceLinkContext;
}

interface IAppToolbarState {
  anchorEl: EventTarget | null;
  aboutPageOpen: boolean;
}

class AppToolbar extends React.PureComponent<IAppToolbarProps, IAppToolbarState> {
  public state = {
    anchorEl: null,
    aboutPageOpen: false,
  };

  public openSettings = (event: React.MouseEvent<HTMLElement>) => {
    this.setState({
      anchorEl: event.currentTarget,
    });
  };

  public closeSettings = () => {
    this.setState({
      anchorEl: null,
    });
  };

  private toggleAboutPage = () => {
    this.setState({
      aboutPageOpen: !this.state.aboutPageOpen,
    });
  };

  public render() {
    const { onMenuIconClick, classes } = this.props;
    const { anchorEl } = this.state;
    const { namespace, isNativeLink } = this.props.context;
    const cdapVersion = VersionStore.getState().version;
    const docsUrl = `http://docs.cdap.io/cdap/${cdapVersion}/en/index.html`;
    return (
      <Toolbar className={classes.customToolbar}>
        <IconButton
          onClick={onMenuIconClick}
          color="inherit"
          className={classnames(classes.iconButton, classes.iconButtonFocus)}
        >
          <MenuIcon />
        </IconButton>
        <div className={classes.grow}>
          <BrandImage />
        </div>
        <div>
          <ToolBarFeatureLink
            featureFlag={Theme.showDashboard}
            featureName={Theme.featureNames.dashboard}
            featureUrl={`/ns/${namespace}/operations`}
          />
          <ToolBarFeatureLink
            featureFlag={Theme.showReports}
            featureName={Theme.featureNames.reports}
            featureUrl={`/ns/${namespace}/reports`}
          />
          <HubButton />
          <Button
            component={isNativeLink ? 'a' : ExtendedLinkButton(`/administration`)}
            className={classnames(classes.buttonLink)}
            href={`/cdap/administration`}
          >
            {Theme.featureNames.systemAdmin}
          </Button>
        </div>
        <div onClick={this.openSettings}>
          <IconButton className={classnames(classes.buttonLink, classes.iconButtonFocus)}>
            <IconSVG name="icon-cogs" className={classes.cogWheelFontSize} />
          </IconButton>
        </div>
        <Menu
          id="simple-menu"
          anchorEl={anchorEl as HTMLElement}
          open={Boolean(anchorEl)}
          onClose={this.closeSettings}
          anchorPosition={{
            left: 0,
            top: 40,
          }}
        >
          <MenuItem>
            <a
              className={classes.anchorMenuItem}
              href={docsUrl}
              target="_blank"
              rel="noopener noreferrer"
            >
              {T.translate('features.Navbar.ProductDropdown.documentationLabel')}
            </a>
          </MenuItem>
          <If condition={Theme.showAboutProductModal === true}>
            <MenuItem onClick={this.toggleAboutPage}>
              <a>
                {T.translate('features.Navbar.ProductDropdown.aboutLabel', {
                  productName: Theme.productName,
                })}
              </a>
            </MenuItem>
          </If>
        </Menu>
        <If condition={Theme.showAboutProductModal === true}>
          <AboutPageModal
            cdapVersion={cdapVersion}
            isOpen={this.state.aboutPageOpen}
            toggle={this.toggleAboutPage}
          />
        </If>
      </Toolbar>
    );
  }
}

export default withStyles(styles)(withContext(AppToolbar));
