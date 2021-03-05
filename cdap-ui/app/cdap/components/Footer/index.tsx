/*
 * Copyright © 2021 Cask Data, Inc.
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
import { Theme } from 'services/ThemeHelper';
import If from 'components/If';
import { objectQuery } from 'services/helpers';
import NamespaceStore, { getCurrentNamespace } from 'services/NamespaceStore';
import { makeStyles } from '@material-ui/core/styles';
import PageTitleStore, { getCurrentPageTitle } from 'services/PageTitleStore/PageTitleStore';
import ThemeWrapper from 'components/ThemeWrapper';

const useStyles = makeStyles((theme) => ({
  root: {
    background: 'white',
    color: theme.palette.grey[400],
    fontSize: '11px',
    fontWeight: 600,
    zIndex: 0,
    position: 'absolute',
    width: '100%',
    bottom: '0',
    borderTop: 'solid 1px ' + theme.palette.grey[300],
  },
  footerText: {
    height: '53px',
    lineHeight: '53px',
    margin: '0',
    textAlign: 'center',
  },
  footerUrl: {
    color: 'inherit',
  },
  instanceMetadataId: {
    position: 'absolute',
    top: '0',
    right: '10px',
    color: theme.palette.grey[400],
    height: '53px',
    lineHeight: '53px',
    margin: '0',
  },
  selectedNamespace: {
    position: 'absolute',
    top: '0',
    left: '10px',
    color: theme.palette.grey[400],
    height: '53px',
    lineHeight: '53px',
    margin: '0',
  },
}));

const nonNamespacePages = ['Operations', 'Reports', 'Administration'];
export default function Footer(props) {
  const footerText = Theme.footerText;
  const footerUrl = Theme.footerLink;
  // 'project-id-30-characters-name1/instance-id-30-characters-name';
  const instanceMetadataId = objectQuery(window, 'CDAP_CONFIG', 'instanceMetadataId');
  const [selectedNamespace, setSelectedNamespace] = React.useState(getCurrentNamespace());
  const classes = useStyles(props);
  React.useEffect(() => {
    const namespaceSub = NamespaceStore.subscribe(() =>
      setSelectedNamespace(getCurrentNamespace())
    );
    const titleSub = PageTitleStore.subscribe(() => {
      const title = getCurrentPageTitle();
      const featureName = (objectQuery(title.split('|'), 1) || '').trim();
      if (nonNamespacePages.indexOf(featureName) !== -1) {
        setSelectedNamespace('--');
      }
    });
    return () => {
      namespaceSub();
      titleSub();
    };
  }, []);
  return (
    <ThemeWrapper>
      <footer className={classes.root}>
        <p className={classes.selectedNamespace}>Namespace: {selectedNamespace}</p>
        <p className={classes.footerText}>
          <a
            href={footerUrl}
            target="_blank"
            rel="noopener noreferrer"
            className={classes.footerUrl}
          >
            {footerText}
          </a>
        </p>
        <If condition={instanceMetadataId}>
          <p className={classes.instanceMetadataId}>Instance Id: {instanceMetadataId}</p>
        </If>
      </footer>
    </ThemeWrapper>
  );
}
