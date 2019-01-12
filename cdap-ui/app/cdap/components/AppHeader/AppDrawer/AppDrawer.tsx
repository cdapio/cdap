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
import List from '@material-ui/core/List';
import ListItemText from '@material-ui/core/ListItemText';
import Drawer from '@material-ui/core/Drawer';
import NamespaceDropdown from 'components/NamespaceDropdown';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
const colorVariables = require('styles/variables.scss');
import { Link } from 'react-router-dom';
import { withContext, INamespaceLinkContext } from 'components/AppHeader/NamespaceLinkContext';
import DrawerFeatureLink from 'components/AppHeader/AppDrawer/DrawerFeatureLink';
import { Theme } from 'services/ThemeHelper';

const DRAWER_WIDTH = '240px';
export const appDrawerListItemTextStyles = {
  fontWeight: 400,
  fontSize: '14px',
};
export const appDrawerListItemStyles = {
  '&:hover': {
    backgroundColor: colorVariables.grey06,
    color: colorVariables.grey02,
  },
};
const styles = (theme) => {
  return {
    drawer: {
      zIndex: theme.zIndex.drawer,
      width: DRAWER_WIDTH,
    },
    drawerPaper: {
      width: DRAWER_WIDTH,
      backgroundColor: colorVariables.grey08,
    },
    listItemText: appDrawerListItemTextStyles,
    toolbar: theme.mixins.toolbar,
    mainMenu: {
      borderTop: `1px solid ${colorVariables.grey06}`,
    },
    namespaceAdminMenu: {
      // WUT TS?
      position: 'absolute' as 'absolute',
      bottom: '130px',
      width: '100%',
      borderTop: `1px solid ${colorVariables.grey06}`,
    },
  };
};

interface IAppDrawerProps extends WithStyles<typeof styles> {
  open: boolean;
  onClose: () => void;
  componentDidNavigate: () => void;
  context: INamespaceLinkContext;
}

class AppDrawer extends React.PureComponent<IAppDrawerProps> {
  public state = {
    onNamespacePreferenceEdit: false,
  };
  public toggleonNamespacePreferenceEdit = () => {
    this.setState({ onNamespacePreferenceEdit: !this.state.onNamespacePreferenceEdit });
  };
  public render() {
    const { classes, open, onClose, componentDidNavigate = () => null } = this.props;
    const { isNativeLink, namespace } = this.props.context;
    const nsurl = `ns/${namespace}`;
    return (
      <Drawer
        open={open}
        onClose={onClose}
        className={classes.drawer}
        disableEnforceFocus={true}
        disableEscapeKeyDown={this.state.onNamespacePreferenceEdit}
        ModalProps={{
          keepMounted: true,
        }}
        classes={{
          paper: classes.drawerPaper,
        }}
        data-cy="navbar-drawer"
      >
        <div className={classes.toolbar} />
        <NamespaceDropdown
          onNamespaceCreate={onClose}
          onNamespacePreferenceEdit={this.toggleonNamespacePreferenceEdit}
          onNamespaceChange={onClose}
          tag={isNativeLink ? 'a' : Link}
        />
        <List component="nav" dense={true} className={classes.mainMenu}>
          <DrawerFeatureLink
            featureName={Theme.featureNames.controlCenter}
            featureFlag={true}
            featureUrl={`/${nsurl}`}
            componentDidNavigate={componentDidNavigate}
            isActive={
              location.pathname === `/cdap/${nsurl}` ||
              location.pathname.startsWith(`/cdap/${nsurl}/dataset`) ||
              location.pathname.startsWith(`/cdap/${nsurl}/apps`)
            }
          />
          <DrawerFeatureLink
            featureName={Theme.featureNames.pipelines}
            featureFlag={Theme.showPipelines}
            featureUrl={`/${nsurl}/pipelines`}
            componentDidNavigate={componentDidNavigate}
            data-cy="navbar-pipelines-link"
            isActive={
              location.pathname.startsWith(`/${nsurl}/pipelines`) ||
              location.pathname.startsWith(`/pipelines/${nsurl}`)
            }
            subMenu={[
              {
                featureName: Theme.featureNames.pipelineStudio,
                featureFlag: Theme.showPipelineStudio,
                featureUrl: `/pipelines/${nsurl}/studio`,
                componentDidNavigate,
                isAngular: true,
                'data-cy': 'navbar-pipeline-studio-link',
              },
            ]}
          />
          <DrawerFeatureLink
            featureName={Theme.featureNames.dataPrep}
            featureFlag={Theme.showDataPrep}
            featureUrl={`/${nsurl}/dataprep`}
            componentDidNavigate={componentDidNavigate}
            isActive={
              location.pathname.startsWith(`/cdap/${nsurl}/dataprep`) ||
              location.pathname.startsWith(`/cdap/${nsurl}/connections`)
            }
          />
          <DrawerFeatureLink
            featureUrl={`/${nsurl}/experiments`}
            featureFlag={Theme.showAnalytics}
            featureName={Theme.featureNames.analytics}
            componentDidNavigate={componentDidNavigate}
          />
          <DrawerFeatureLink
            featureUrl={`/${nsurl}/rulesengine`}
            featureFlag={Theme.showRulesEngine}
            featureName={Theme.featureNames.rulesEngine}
            componentDidNavigate={componentDidNavigate}
          />
          <DrawerFeatureLink
            featureUrl={`/metadata/${nsurl}`}
            featureFlag={Theme.showMetadata}
            featureName={Theme.featureNames.metadata}
            isAngular={true}
          />
        </List>
        <List component="nav" dense={true} className={classes.namespaceAdminMenu}>
          <DrawerFeatureLink
            featureUrl="/administration/configuration"
            featureName={Theme.featureNames.projectAdmin}
            featureFlag={true}
            componentDidNavigate={componentDidNavigate}
          />
        </List>
      </Drawer>
    );
  }
}

export default withStyles(styles)(withContext(AppDrawer));
