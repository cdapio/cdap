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
import ListItemLink from 'components/AppHeader/ListItemLink';
import List from '@material-ui/core/List';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import { Link } from 'react-router-dom';

const styles = (theme) => {
  return {
    listItemText: {
      fontWeight: 600,
      fontSize: '1rem',
    },
    nestListPadding: {
      paddingLeft: theme.spacing.unit * 4,
    },
  };
};

interface IDrawerFeatureLinkProps extends WithStyles<typeof styles> {
  context: INamespaceLinkContext;
  componentDidNavigate?: () => void;
  featureFlag: boolean;
  featureName: string;
  featureUrl: string;
  isAngular?: boolean;
  subMenu?: IDrawerFeatureLinkProps[];
}

class DrawerFeatureLink extends React.PureComponent<IDrawerFeatureLinkProps> {
  private renderListItem(
    {
      componentDidNavigate,
      featureFlag,
      featureName,
      featureUrl,
      isAngular,
    }: IDrawerFeatureLinkProps,
    isSubMenu = false
  ) {
    const { isNativeLink } = this.props.context;
    const { classes } = this.props;
    if (featureFlag === false) {
      return null;
    }
    return (
      <ListItemLink
        className={isSubMenu ? classes.nestListPadding : ''}
        component={isNativeLink || isAngular ? 'a' : Link}
        href={isAngular ? featureUrl : `/cdap${featureUrl}`}
        to={featureUrl}
        onClick={componentDidNavigate}
      >
        <ListItemText
          disableTypography
          classes={{ root: classes.listItemText }}
          primary={featureName}
        />
      </ListItemLink>
    );
  }
  private renderSubMenu() {
    const { subMenu = [] } = this.props;
    if (!subMenu.length) {
      return null;
    }

    return (
      <List disablePadding>
        {subMenu.map((menu, i) => (
          <React.Fragment key={i}> {this.renderListItem(menu, true)}</React.Fragment>
        ))}
      </List>
    );
  }
  public render() {
    return (
      <React.Fragment>
        {this.renderListItem(this.props)}
        {this.renderSubMenu()}
      </React.Fragment>
    );
  }
}

const DrawerFeatureLinkWithContext = withStyles(styles)(withContext(DrawerFeatureLink));

export default DrawerFeatureLinkWithContext;
