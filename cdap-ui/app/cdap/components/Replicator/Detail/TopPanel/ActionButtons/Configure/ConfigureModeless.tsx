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
import { detailContextConnect, IDetailContext } from 'components/Replicator/Detail';
import ProfilesListViewInPipeline from 'components/PipelineDetails/ProfilesListView';
import Heading, { HeadingTypes } from 'components/Heading';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { MyPreferenceApi } from 'api/preference';
import pickBy from 'lodash/pickBy';
import mapKeys from 'lodash/mapKeys';
import Button from '@material-ui/core/Button';

const modelessWidth = '600px';
const buttonWidth = '70px';
const headerHeight = '50px';

const styles = (theme): StyleRules => {
  return {
    root: {
      position: 'absolute',
      width: modelessWidth,
      backgroundColor: theme.palette.white[50],
      boxShadow: '0 2px 4px rgba(0,0,0,.5)',
      top: '10px',
      right: `calc(-${modelessWidth} / 2 + ${buttonWidth} / 2)`,
      zIndex: 6,
    },
    header: {
      height: headerHeight,
      backgroundColor: theme.palette.grey[600],
      display: 'flex',
      alignItems: 'center',
      paddingLeft: '15px',
    },
    content: {
      height: '400px',
      padding: '10px 15px',
    },
    footer: {
      padding: '10px 15px',
      '& > *': {
        marginRight: '5px',
      },
    },
  };
};

interface IActiveProfile {
  name?: string;
  profileCustomizations?: Record<string, string>;
}

interface IConfigureModelessProps extends IDetailContext, WithStyles<typeof styles> {
  onToggle: () => void;
}

const PROFILE_PREFIX = 'system.profile';
const PROFILE_NAME = `${PROFILE_PREFIX}.name`;
const PROPERTIES_PREFIX = `${PROFILE_PREFIX}.properties.`;

const ConfigureModelessView: React.FC<IConfigureModelessProps> = ({ classes, name, onToggle }) => {
  const [activeProfile, setActiveProfile] = React.useState<IActiveProfile>({});
  const [loading, setLoading] = React.useState(false);

  React.useEffect(() => {
    const params = {
      namespace: getCurrentNamespace(),
      appId: name,
    };

    MyPreferenceApi.getAppPreferencesResolved(params).subscribe((preferences) => {
      const profileName = preferences[PROFILE_NAME];

      if (!profileName) {
        return;
      }

      const customProperties = pickBy(preferences, (value, key) => {
        return key.startsWith(PROPERTIES_PREFIX);
      });

      const customizations = mapKeys(customProperties, (value, key) => {
        return key.slice(PROPERTIES_PREFIX.length);
      });

      setActiveProfile({
        name: profileName,
        profileCustomizations: customizations,
      });
    });
  }, []);

  function handleProfileSelect(profileName, customizations = {}) {
    setActiveProfile({
      name: profileName,
      profileCustomizations: customizations,
    });
  }

  function handleSave() {
    if (!activeProfile || !activeProfile.name) {
      return;
    }

    setLoading(true);

    const params = {
      namespace: getCurrentNamespace(),
      appId: name,
    };

    MyPreferenceApi.getAppPreferences(params).subscribe((appPreferences) => {
      const existingPreferences = pickBy(appPreferences, (value, key) => {
        return key !== PROFILE_NAME && !key.startsWith(PROPERTIES_PREFIX);
      });

      const customProperties = mapKeys(activeProfile.profileCustomizations, (value, key) => {
        return `${PROPERTIES_PREFIX}${key}`;
      });

      const preferences = {
        ...existingPreferences,
        ...customProperties,
        [PROFILE_NAME]: activeProfile.name,
      };

      MyPreferenceApi.setAppPreferences(params, preferences).subscribe(
        () => {
          onToggle();
          setLoading(false);
        },
        (err) => {
          setLoading(false);
          // tslint:disable-next-line: no-console
          console.log('err', err);
        }
      );
    });
  }

  return (
    <div className={classes.root}>
      <div className={classes.header}>
        <Heading type={HeadingTypes.h4} label="Configure" />
      </div>

      <div className={classes.content}>
        <ProfilesListViewInPipeline
          appName={name}
          onProfileSelect={handleProfileSelect}
          selectedProfile={activeProfile}
          tableTitle={'Select the compute profile you want to use to run this replicator'}
        />
      </div>

      <div className={classes.footer}>
        <Button variant="contained" color="primary" onClick={handleSave} disabled={loading}>
          Save
        </Button>
        <Button color="primary" onClick={onToggle}>
          Cancel
        </Button>
      </div>
    </div>
  );
};

const StyledConfigureModeless = withStyles(styles)(ConfigureModelessView);
const ConfigureModeless = detailContextConnect(StyledConfigureModeless);
export default ConfigureModeless;
