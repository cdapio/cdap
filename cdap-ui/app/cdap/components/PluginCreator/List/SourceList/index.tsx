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
import { getCurrentNamespace } from 'services/NamespaceStore';
import { Link } from 'react-router-dom';
import PluginCard, {
  PluginCardWidth,
  PluginCardHeight,
} from 'components/PluginCreator/List/PluginCard';
import HorizontalCarousel from 'components/HorizontalCarousel';
import { PluginType } from 'components/PluginCreator/constants';
import TextField from '@material-ui/core/TextField';
import InputAdornment from '@material-ui/core/InputAdornment';
import Search from '@material-ui/icons/Search';
import { objectQuery } from 'services/helpers';
import If from 'components/If';
import LoadingSVG from 'components/LoadingSVG';
import { MyReplicatorApi } from 'api/replicator';

const styles = (): StyleRules => {
  return {
    header: {
      display: 'grid',
      gridTemplateColumns: '1fr 400px',
    },
    link: {
      marginRight: '25px',
      display: 'inline-block',

      '&:hover': {
        textDecoration: 'none',
      },
    },
    listContainer: {
      marginTop: '15px',
      width: '100%',
      height: '100%',
      display: 'table-cell',
      verticalAlign: 'middle',
    },
    arrow: {
      top: `${Math.floor(PluginCardHeight / 2)}px`,
    },
  };
};

const SourceListView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  return (
    <div>
      <div className={classes.header}>
        <div>
          <h4>Create new plugin JSON</h4>
          <div>Start by selecting the method of creating the JSON.</div>
        </div>
      </div>

      <div className={classes.listContainer}>
        <div>
          <Link
            key={2}
            className={classes.link}
            to={`/ns/${getCurrentNamespace()}/plugincreation/create`}
          >
            <PluginCard
              name={'Create From Scratch'}
              icon={
                'https://encrypted-tbn0.gstatic.com/images?q=tbn%3AANd9GcTid6tqhgxOvmcLDGgwpj-S1_UsX827-Wqc4A4gDEombVJh9zzb&usqp=CAU'
              }
            />
          </Link>
        </div>
      </div>
    </div>
  );
};

const SourceList = withStyles(styles)(SourceListView);
export default SourceList;
