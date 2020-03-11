/*
 * Copyright © 2020 Cask Data, Inc.
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
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { MarkdownWithStyles } from 'components/Markdown';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { MyReplicatorApi } from 'api/replicator';

const styles = (): StyleRules => {
  return {
    root: {
      padding: '0 10px',
    },
  };
};

interface IDocumentationProps extends WithStyles<typeof styles> {
  pluginInfo: {
    name: string;
    type: string;
    artifact: {
      name: string;
      version: string;
      scope: string;
    };
  };
}

const DocumentationView: React.FC<IDocumentationProps> = ({ classes, pluginInfo }) => {
  const [docs, setDocs] = React.useState('');

  React.useEffect(() => {
    const params = {
      namespace: getCurrentNamespace(),
      artifactName: pluginInfo.artifact.name,
      artifactVersion: pluginInfo.artifact.version,
      scope: pluginInfo.artifact.scope,
      keys: `doc.${pluginInfo.name}-${pluginInfo.type}`,
    };

    MyReplicatorApi.fetchArtifactProperties(params).subscribe((res) => {
      setDocs(res[params.keys]);
    });
  }, []);

  return (
    <div className={classes.root}>
      <MarkdownWithStyles markdown={docs} />
    </div>
  );
};

const Documentation = withStyles(styles)(DocumentationView);
export default Documentation;
