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

import ProfilesStore, {PROFILES_ACTIONS} from 'components/Cloud/Profiles/Store';
import {MyCloudApi} from 'api/cloud';
import {MyPreferenceApi} from 'api/preference';
import fileDownload from 'js-file-download';
import {objectQuery} from 'services/helpers';
import {Observable} from 'rxjs/Observable';
import {CLOUD} from 'services/global-constants';

export const getProfiles = (namespace) => {
  ProfilesStore.dispatch({
    type: PROFILES_ACTIONS.SET_LOADING,
    payload: {
      loading: true
    }
  });

  let profileObservable = MyCloudApi.getSystemProfiles();
  if (namespace !== 'system') {
    profileObservable = profileObservable.combineLatest(MyCloudApi.list({ namespace }));
  } else {
    profileObservable = profileObservable.combineLatest(Observable.of([]));
  }

  profileObservable
    .subscribe(
      ([systemProfiles = [], namespaceProfiles = []]) => {
        let profiles = namespaceProfiles.concat(systemProfiles);
        ProfilesStore.dispatch({
          type: PROFILES_ACTIONS.SET_PROFILES,
          payload: { profiles }
        });
      },
      setError
    );
};

export const exportProfile = (namespace, profile) => {
  MyCloudApi
    .get({
      namespace,
      profile: profile.name
    })
    .subscribe(
      (res) => {
        let json = JSON.stringify(res, null, 2);
        let fileName = `${profile.name}-${profile.provisioner.name}-profile.json`;
        fileDownload(json, fileName);
      },
      setError
    );
};

export const deleteProfile = (namespace, profile, currentNamespace) => {
  let deleteObservable = MyCloudApi.delete({
    namespace,
    profile
  });
  deleteObservable.subscribe(
    () => {
      getProfiles(currentNamespace);
    },
    (err) => {
      Observable.throw(err);
    });
  return deleteObservable;
};

export const importProfile = (namespace, e) => {
  if (!objectQuery(e, 'target', 'files', 0)) {
    return;
  }

  let uploadedFile = e.target.files[0];
  let reader = new FileReader();
  reader.readAsText(uploadedFile, 'UTF-8');

  reader.onload =  (evt) => {
    let jsonSpec = evt.target.result;
    try {
      jsonSpec = JSON.parse(jsonSpec);
    } catch (error) {
      ProfilesStore.dispatch({
        type: PROFILES_ACTIONS.SET_ERROR,
        payload: {
          error: error.message || error
        }
      });
      return;
    }

    MyCloudApi
      .create({
        namespace,
        profile: jsonSpec.name
      }, jsonSpec)
      .subscribe(
        () => {
          getProfiles(namespace);
          let profilePrefix = namespace === 'system' ? 'SYSTEM' : 'USER';
          let profileName = `${profilePrefix}:${jsonSpec.name}`;
          highlightNewProfile(profileName);
        },
        (error) => {
          ProfilesStore.dispatch({
            type: PROFILES_ACTIONS.SET_ERROR,
            payload: {
              error: error.response || error
            }
          });
        }
      );
  };
};

export const setError = (error = null) => {
  ProfilesStore.dispatch({
    type: PROFILES_ACTIONS.SET_ERROR,
    payload: { error }
  });
};

export const resetProfiles = () => {
  ProfilesStore.dispatch({
    type: PROFILES_ACTIONS.RESET
  });
};

export const getProvisionerLabel = (profile, provisioners) => {
  if (provisioners.length) {
    let matchingProvisioner = provisioners.find((prov) => {
      return prov.name === profile.provisioner.name;
    });
    if (matchingProvisioner) {
      return matchingProvisioner.label;
    }
  }
  return profile.provisioner.name;
};

export const extractProfileName = (name = '') => {
  return name.replace(/(USER|SYSTEM):/g, '');
};

export const getProfileNameWithScope = (name = '', scope) => {
  if (name && scope) {
    if (scope === 'SYSTEM') {
      return `SYSTEM:${name}`;
    }
    return `USER:${name}`;
  }
  return name;
};

export const isSystemProfile = (name = '') => {
  return name.indexOf('SYSTEM:') === 0;
};

export const getDefaultProfile = (namespace) => {
  let preferenceApi;

  if (namespace === 'system') {
    preferenceApi = MyPreferenceApi.getSystemPreferences();
  } else {
    preferenceApi = MyPreferenceApi.getNamespacePreferences({namespace});
  }

  preferenceApi
    .subscribe(
      (preferences = {}) => {
        let defaultProfile = preferences[CLOUD.PROFILE_NAME_PREFERENCE_PROPERTY];
        if (!defaultProfile && namespace === 'system') {
          defaultProfile = CLOUD.DEFAULT_PROFILE_NAME;
        }
        if (defaultProfile) {
          ProfilesStore.dispatch({
            type: PROFILES_ACTIONS.SET_DEFAULT_PROFILE,
            payload: { defaultProfile }
          });
        }
      },
      setError
    );
};

export const setDefaultProfile = (namespace, profileName) => {
  let postBody = {
    [CLOUD.PROFILE_NAME_PREFERENCE_PROPERTY]: profileName
  };

  let preferenceApi;

  if (namespace === 'system') {
    preferenceApi = MyPreferenceApi.setSystemPreferences({}, postBody);
  } else {
    preferenceApi = MyPreferenceApi.setNamespacePreferences({namespace}, postBody);
  }

  preferenceApi
    .subscribe(
      () => {
        ProfilesStore.dispatch({
          type: PROFILES_ACTIONS.SET_DEFAULT_PROFILE,
          payload: { defaultProfile: profileName }
        });
      },
      setError
    );
};

export const highlightNewProfile = (profileName) => {
  ProfilesStore.dispatch({
    type: PROFILES_ACTIONS.SET_NEW_PROFILE,
    payload: {
      newProfile: profileName
    }
  });
  setTimeout(() => {
    ProfilesStore.dispatch({
      type: PROFILES_ACTIONS.SET_NEW_PROFILE,
      payload: {
        newProfile: null
      }
    });
  }, 3000);
};
