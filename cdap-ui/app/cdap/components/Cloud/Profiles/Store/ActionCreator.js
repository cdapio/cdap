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
import fileDownload from 'js-file-download';
import {objectQuery} from 'services/helpers';
import {Observable} from 'rxjs/Observable';

export const getProfiles = (namespace) => {
  ProfilesStore.dispatch({
    type: PROFILES_ACTIONS.SET_LOADING,
    payload: {
      loading: true
    }
  });

  let profileObservable = MyCloudApi.list({ namespace: 'system' });
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
      (error) => {
        ProfilesStore.dispatch({
          type: PROFILES_ACTIONS.SET_ERROR,
          payload: { error }
        });
      }
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
      (error) => {
        ProfilesStore.dispatch({
          type: PROFILES_ACTIONS.SET_ERROR,
          payload: { error }
        });
      }
    );
};

export const deleteProfile = (namespace, profile, currentNamespace) => {
  let deleteObservable = MyCloudApi.delete({
    namespace,
    profile
  });
  deleteObservable.subscribe(() => {
    getProfiles(currentNamespace);
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

export const setError = (error) => {
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
