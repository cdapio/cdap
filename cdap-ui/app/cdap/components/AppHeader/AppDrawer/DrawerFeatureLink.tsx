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
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import { Link } from 'react-router-dom';

const styles = () => {
  return {
    listItemText: {
      fontWeight: 600,
      fontSize: '1rem',
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
}

class DrawerFeatureLink extends React.PureComponent<IDrawerFeatureLinkProps> {
  public render() {
    const {
      classes,
      componentDidNavigate = () => null,
      featureFlag,
      featureName,
      featureUrl,
      isAngular = false,
    } = this.props;
    if (featureFlag === false) {
      return null;
    }

    const { isNativeLink } = this.props.context;

    return (
      <ListItemLink
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
}

const DrawerFeatureLinkWithContext = withStyles(styles)(withContext(DrawerFeatureLink));

export default DrawerFeatureLinkWithContext;
