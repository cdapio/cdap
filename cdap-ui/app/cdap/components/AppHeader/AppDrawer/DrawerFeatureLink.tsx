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
import { withContext, INamespaceLinkContext } from 'components/AppHeader/NamespaceLinkContext';
import ListItemText from '@material-ui/core/ListItemText';
import ListItem from '@material-ui/core/ListItem';
import ListItemLink from 'components/AppHeader/ListItemLink';
import List from '@material-ui/core/List';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import { Link } from 'react-router-dom';
import {
  appDrawerListItemTextStyles,
  appDrawerListItemStyles,
} from 'components/AppHeader/AppDrawer/AppDrawer';
import classnames from 'classnames';
import isEmpty from 'lodash/isEmpty';
import Collapse from '@material-ui/core/Collapse';
import IconSVG from 'components/IconSVG';
import If from 'components/If';

const colorVariables = require('styles/variables.scss');

const styles = (theme) => {
  return {
    listItemText: appDrawerListItemTextStyles(theme),
    listItem: appDrawerListItemStyles(theme),
    nestListPadding: {
      paddingLeft: theme.Spacing(6),
    },
    listItemWithSubmenu: {
      cursor: 'pointer',
      ...appDrawerListItemStyles(theme),
    },
    featureIconSize: {
      fontSize: '1.3rem',
    },
    activeListItem: {
      backgroundColor: colorVariables.bluegrey06,
      color: colorVariables.blue02,
    },
  };
};

interface IDrawerFeatureLinkProps extends WithStyles<typeof styles> {
  context: INamespaceLinkContext;
  componentDidNavigate?: () => void;
  featureFlag: boolean;
  featureName: string;
  featureSVGIconName?: string;
  featureUrl: string;
  isAngular?: boolean;
  isActive?: boolean;
  subMenu?: IDrawerFeatureLinkProps[];
  'data-cy'?: string;
  id: string;
}
interface IDrawerFeatureLinkState {
  submenuOpen: boolean;
}

class DrawerFeatureLink extends React.PureComponent<
  IDrawerFeatureLinkProps,
  IDrawerFeatureLinkState
> {
  public state: IDrawerFeatureLinkState = {
    submenuOpen: true,
  };

  private toggleSubmenuOpen = () => {
    this.setState({
      submenuOpen: !this.state.submenuOpen,
    });
  };

  private renderCaretForSubmenu = () => {
    return <IconSVG name={this.state.submenuOpen ? 'icon-caret-up' : 'icon-caret-down'} />;
  };

  private renderListItem(
    {
      componentDidNavigate,
      featureFlag,
      featureName,
      featureSVGIconName,
      featureUrl,
      isAngular,
      isActive,
      id,
      ...rest
    }: IDrawerFeatureLinkProps,
    isSubMenu = false
  ) {
    const { isNativeLink } = this.props.context;
    const { classes } = this.props;
    const { pathname } = location;
    const reactFeatureUrl = `/cdap${featureUrl}`;
    const activeFeatureUrl = isAngular ? featureUrl : reactFeatureUrl;
    const localIsActive =
      typeof isActive === 'undefined' ? pathname.startsWith(activeFeatureUrl) : isActive;
    if (featureFlag === false) {
      return null;
    }
    return (
      <ListItemLink
        id={id}
        className={classnames(classes.listItem, {
          [classes.nestListPadding]: isSubMenu,
          [classes.activeListItem]: localIsActive,
        })}
        component={isNativeLink || isAngular ? 'a' : Link}
        href={isAngular ? featureUrl : reactFeatureUrl}
        to={featureUrl}
        onClick={componentDidNavigate}
        data-cy={rest['data-cy']}
      >
        <If condition={typeof featureSVGIconName === 'string'}>
          <IconSVG className={classes.featureIconSize} name={featureSVGIconName || ''} />
        </If>
        <ListItemText
          disableTypography
          classes={{ root: classes.listItemText }}
          primary={featureName}
        />
      </ListItemLink>
    );
  }
  private renderSubMenu() {
    const { subMenu = [], featureName, classes, featureSVGIconName } = this.props;
    if (!subMenu.length) {
      return null;
    }

    return (
      <React.Fragment>
        <ListItem onClick={this.toggleSubmenuOpen} className={classes.listItemWithSubmenu}>
          <If condition={typeof featureSVGIconName === 'string'}>
            <IconSVG className={classes.featureIconSize} name={featureSVGIconName} />
          </If>
          <ListItemText
            disableTypography
            classes={{ root: classes.listItemText }}
            primary={featureName}
          />
          {this.renderCaretForSubmenu()}
        </ListItem>
        <Collapse in={this.state.submenuOpen} timeout="auto" unmountOnExit>
          <List disablePadding>
            {subMenu.map((menu, i) => (
              <React.Fragment key={i}> {this.renderListItem(menu, true)}</React.Fragment>
            ))}
          </List>
        </Collapse>
      </React.Fragment>
    );
  }
  public render() {
    return (
      <React.Fragment>
        {isEmpty(this.props.subMenu) ? this.renderListItem(this.props) : this.renderSubMenu()}
      </React.Fragment>
    );
  }
}

const DrawerFeatureLinkWithContext = withStyles(styles)(withContext(DrawerFeatureLink));

export default DrawerFeatureLinkWithContext;
