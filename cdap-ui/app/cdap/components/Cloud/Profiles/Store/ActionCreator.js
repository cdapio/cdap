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

import ProfilesStore, { PROFILES_ACTIONS } from 'components/Cloud/Profiles/Store';
import { MyCloudApi } from 'api/cloud';
import { MyPreferenceApi } from 'api/preference';
import fileDownload from 'js-file-download';
import { objectQuery } from 'services/helpers';
import { Observable } from 'rxjs/Observable';
import { CLOUD } from 'services/global-constants';
import { MyMetricApi } from 'api/metric';
import { MySearchApi } from 'api/search';
import { GLOBALS, SYSTEM_NAMESPACE } from 'services/global-constants';
import isNil from 'lodash/isNil';
import { SCOPES } from 'services/global-constants';
import { Theme } from 'services/ThemeHelper';

export const getProfileMetricsBody = (
  queryId,
  namespace,
  profilescope,
  startTime,
  endTime,
  extraTags = {}
) => {
  let tags = {
    profilescope,
    ...extraTags,
  };
  if (namespace !== SYSTEM_NAMESPACE) {
    tags.namespace = namespace;
  }
  let metricBody = {
    [queryId]: {
      tags,
      metrics: [
        'system.program.completed.runs',
        'system.program.failed.runs',
        'system.program.killed.runs',
        'system.program.node.minutes',
      ],
      timeRange: {
        start: startTime,
        end: endTime,
        resolution: 'auto',
      },
      groupBy: ['profile'],
    },
  };
  if (!startTime && !endTime) {
    metricBody = {
      ...metricBody,
      [queryId]: {
        ...metricBody[queryId],
        timeRange: {
          ...metricBody[queryId].timeRange,
          resolution: '1h',
          aggregate: true,
        },
      },
    };
  }
  return metricBody;
};

const convertMetadataToAssociations = (metadata) => {
  let schedulesCount = 0,
    triggersCount = 0;
  metadata.forEach((m) => {
    const schedule = objectQuery(m, 'entity', 'details', 'schedule');
    if (schedule) {
      // fixed name for time based schedule.
      if (schedule === GLOBALS.defaultScheduleId) {
        schedulesCount += 1;
      } else {
        triggersCount += 1;
      }
    }
  });
  return { schedulesCount, triggersCount };
};

const updateScheduleAndTriggersToStore = (profileName, metadata) => {
  let { schedulesCount, triggersCount } = convertMetadataToAssociations(metadata);
  ProfilesStore.dispatch({
    type: PROFILES_ACTIONS.SET_SCHEDULES_TRIGGERS_COUNT,
    payload: {
      profile: profileName,
      schedulesCount,
      triggersCount,
    },
  });
};
export const ONEDAYMETRICKEY = 'oneDayMetrics';
export const OVERALLMETRICKEY = 'overAllMetrics';
export const fetchAggregateProfileMetrics = (namespace, profile, extraTags) => {
  let oneDayMetricsRequestBody = getProfileMetricsBody(
    ONEDAYMETRICKEY,
    namespace,
    profile.scope,
    'now-24h',
    'now',
    extraTags
  );
  let overAllMetricsRequestBody = getProfileMetricsBody(
    OVERALLMETRICKEY,
    namespace,
    profile.scope,
    0,
    0,
    extraTags
  );
  return MyMetricApi.query(null, {
    ...oneDayMetricsRequestBody,
    ...overAllMetricsRequestBody,
  }).flatMap((metrics) => {
    let metricsMap = {
      [ONEDAYMETRICKEY]: {
        runs: '--',
        minutes: '--',
      },
      [OVERALLMETRICKEY]: {
        runs: '--',
        minutes: '--',
      },
    };
    Object.keys(metrics).forEach((query) => {
      metrics[query].series.forEach((metric) => {
        let metricName = metric.metricName.split('.').pop();
        let metricValue;
        if (!metric.data.length) {
          metricValue = 0;
        } else {
          if (metric.data.length === 1) {
            metricValue = metric.data[0].value;
          } else {
            metricValue = metric.data.reduce((prev, curr) => prev + curr.value, 0);
          }
        }
        if (!metricsMap.hasOwnProperty(query)) {
          metricsMap[query] = {};
        }
        if (metricsMap[query][metricName] !== '--' && !isNil(metricsMap[query][metricName])) {
          metricsMap[query][metricName] += metricValue;
        } else {
          metricsMap[query] = {
            ...metricsMap[query],
            [metricName]: metricValue,
          };
        }
      });
    });
    return Observable.create((observer) => {
      observer.next(metricsMap);
    });
  });
};

export const getProfiles = (namespace) => {
  ProfilesStore.dispatch({
    type: PROFILES_ACTIONS.SET_LOADING,
    payload: {
      loading: true,
    },
  });

  let profileObservable = MyCloudApi.getSystemProfiles();
  if (namespace !== SYSTEM_NAMESPACE) {
    profileObservable = profileObservable.combineLatest(MyCloudApi.list({ namespace }));
  } else {
    profileObservable = profileObservable.combineLatest(Observable.of([]));
  }

  profileObservable.subscribe(([systemProfiles = [], namespaceProfiles = []]) => {
    let filteredSystemProfiles = systemProfiles;
    if (Theme.showNativeProfile === false) {
      filteredSystemProfiles = systemProfiles.filter((profile) => profile.name !== 'native');
    }

    let profiles = namespaceProfiles.concat(filteredSystemProfiles).map((profile) => ({
      ...profile,
      oneDayMetrics: {},
      overAllMetrics: {},
      schedulesCount: '--',
      triggersCount: '--',
    }));
    ProfilesStore.dispatch({
      type: PROFILES_ACTIONS.SET_PROFILES,
      payload: { profiles },
    });
    /*
          One metric call with 4 different queries
            Context: namespace, profile scope
            Groupby: profile
            TimeRange:
              - USER scope
                - 24hrs
                - full entirety
              - SYSTEM scope
                - 24hrs
                - full entirety

            One metadata call for entityScope=USER for schedules and triggers count
        */
    const extraTags = {
      programtype: 'Workflow',
    };
    let oneDayUSERMetricsBody = getProfileMetricsBody(
      'oneDayUSERMetrics',
      namespace,
      SCOPES.USER,
      'now-24h',
      'now',
      extraTags
    );
    let overAllUSERMetricsBody = getProfileMetricsBody(
      'overAllUSERMetrics',
      namespace,
      SCOPES.USER,
      0,
      0,
      extraTags
    );
    let oneDaySYSTEMMetricsBody = getProfileMetricsBody(
      'oneDaySYSTEMMetrics',
      namespace,
      SCOPES.SYSTEM,
      'now-24h',
      'now',
      extraTags
    );
    let overAllSYSTEMMetricsBody = getProfileMetricsBody(
      'overAllSYSTEMMetrics',
      namespace,
      SCOPES.SYSTEM,
      0,
      0,
      extraTags
    );
    MyMetricApi.query(null, {
      ...oneDayUSERMetricsBody,
      ...overAllUSERMetricsBody,
      ...oneDaySYSTEMMetricsBody,
      ...overAllSYSTEMMetricsBody,
    }).subscribe((metrics) => {
      let profilesToMetricsMap = {};
      Object.keys(metrics).forEach((query) => {
        // oneDayMetrics, overMetrics are the keys for metrics for each profile.
        let profileScope = query.indexOf(SCOPES.USER) !== -1 ? SCOPES.USER : SCOPES.SYSTEM;
        let metricsKey = query.replace(/USER|SYSTEM/, '');
        metrics[query].series.forEach((metric) => {
          let profileName = metric.grouping.profile;
          /*
                  The map index cannot be just profile name as
                  two different profiles (in user and system scopes)
                  can have the same name. This will overwrite the one from the other scope.
                  Hence the addition of scope to make sure the metrics are not overwritten.
                */
          let profileKey = `${profileScope}:${profileName}`;
          let metricName = metric.metricName.split('.').pop();
          let metricValue;
          if (!metric.data.length) {
            metricValue = 0;
          } else {
            if (metric.data.length === 1) {
              metricValue = metric.data[0].value;
            } else {
              metricValue = metric.data.reduce((prev, curr) => prev + curr.value, 0);
            }
          }
          if (!profilesToMetricsMap.hasOwnProperty(profileKey)) {
            profilesToMetricsMap[profileKey] = { [metricsKey]: {} };
          }
          if (!isNil(objectQuery(profilesToMetricsMap, profileKey, metricsKey, metricName))) {
            metricValue += profilesToMetricsMap[profileKey][metricsKey][metricName];
          }
          /*
                  {
                    SYSTEM:profile1: {
                      oneDayMetrics: {
                        runs: 1,
                        minutes: 2
                      },
                      overAllMetrics: {
                        runs: 2,
                        minutes: 4
                      }
                    }
                  }
                */
          profilesToMetricsMap[profileKey] = {
            ...profilesToMetricsMap[profileKey],
            [metricsKey]: {
              ...profilesToMetricsMap[profileKey][metricsKey],
              [metricName]: metricValue,
            },
          };
        });
      });
      ProfilesStore.dispatch({
        type: PROFILES_ACTIONS.SET_PROFILE_METRICS,
        payload: {
          profilesToMetricsMap,
        },
      });
    });
    profiles.forEach((profile) => {
      let { scope } = profile;
      let profileName = `profile:${scope}:${profile.name}`;
      let apiObservable$;
      if (namespace === SYSTEM_NAMESPACE) {
        apiObservable$ = MySearchApi.searchSystem({
          query: profileName,
          responseFormat: 'v6',
        });
      } else {
        apiObservable$ = MySearchApi.search({
          namespace,
          query: profileName,
          responseFormat: 'v6',
        });
      }
      apiObservable$.subscribe((res) =>
        updateScheduleAndTriggersToStore(profile.name, res.results)
      );
    });
  }, setError);
};

export const exportProfile = (namespace, profile) => {
  let apiObservable$ = MyCloudApi.get({ namespace, profile: profile.name });
  if (namespace === SYSTEM_NAMESPACE) {
    apiObservable$ = MyCloudApi.getSystemProfile({ profile: profile.name });
  }
  apiObservable$.subscribe((res) => {
    let json = JSON.stringify(res, null, 2);
    let fileName = `${profile.name}-${profile.provisioner.name}-profile.json`;
    fileDownload(json, fileName);
  }, setError);
};

export const deleteProfile = (namespace, profile, currentNamespace) => {
  let deleteObservable$;
  if (namespace === SYSTEM_NAMESPACE) {
    deleteObservable$ = MyCloudApi.deleteSystemProfile({
      profile,
    });
  } else {
    deleteObservable$ = MyCloudApi.delete({
      namespace,
      profile,
    });
  }
  deleteObservable$.subscribe(
    () => {
      getProfiles(currentNamespace);
    },
    (err) => {
      Observable.throw(err);
    }
  );
  return deleteObservable$;
};

export const importProfile = (namespace, e) => {
  if (!objectQuery(e, 'target', 'files', 0)) {
    return;
  }

  let uploadedFile = e.target.files[0];
  let reader = new FileReader();
  reader.readAsText(uploadedFile, 'UTF-8');

  reader.onload = (evt) => {
    let jsonSpec = evt.target.result;
    try {
      jsonSpec = JSON.parse(jsonSpec);
    } catch (error) {
      ProfilesStore.dispatch({
        type: PROFILES_ACTIONS.SET_ERROR,
        payload: {
          error: error.message || error,
        },
      });
      return;
    }
    let apiObservable$ = MyCloudApi.create(
      {
        namespace,
        profile: jsonSpec.name,
      },
      jsonSpec
    );
    if (namespace === SYSTEM_NAMESPACE) {
      apiObservable$ = MyCloudApi.createSystemProfile(
        {
          profile: jsonSpec.name,
        },
        jsonSpec
      );
    }
    apiObservable$.subscribe(
      () => {
        getProfiles(namespace);
        let profilePrefix = namespace === SYSTEM_NAMESPACE ? SCOPES.SYSTEM : SCOPES.USER;
        let profileName = `${profilePrefix}:${jsonSpec.name}`;
        highlightNewProfile(profileName);
      },
      (error) => {
        ProfilesStore.dispatch({
          type: PROFILES_ACTIONS.SET_ERROR,
          payload: {
            error: error.response || error,
          },
        });
      }
    );
  };
};

export const setError = (error = null) => {
  ProfilesStore.dispatch({
    type: PROFILES_ACTIONS.SET_ERROR,
    payload: { error },
  });
};

export const resetProfiles = () => {
  ProfilesStore.dispatch({
    type: PROFILES_ACTIONS.RESET,
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
    if (scope === SCOPES.SYSTEM) {
      return `${SCOPES.SYSTEM}:${name}`;
    }
    return `${SCOPES.USER}:${name}`;
  }
  return name;
};

export const isSystemProfile = (name = '') => {
  return name.indexOf('SYSTEM:') === 0;
};

export const getDefaultProfile = (namespace) => {
  let preferenceApi;

  if (namespace === SYSTEM_NAMESPACE) {
    preferenceApi = MyPreferenceApi.getSystemPreferences();
  } else {
    preferenceApi = MyPreferenceApi.getNamespacePreferences({ namespace });
  }

  preferenceApi.subscribe((preferences = {}) => {
    let defaultProfile = preferences[CLOUD.PROFILE_NAME_PREFERENCE_PROPERTY];
    if (!defaultProfile && namespace === SYSTEM_NAMESPACE) {
      defaultProfile = CLOUD.DEFAULT_PROFILE_NAME;
    }
    if (defaultProfile) {
      ProfilesStore.dispatch({
        type: PROFILES_ACTIONS.SET_DEFAULT_PROFILE,
        payload: { defaultProfile },
      });
    }
  }, setError);
};

export const setDefaultProfile = (namespace, profileName) => {
  function successCallback() {
    ProfilesStore.dispatch({
      type: PROFILES_ACTIONS.SET_DEFAULT_PROFILE,
      payload: { defaultProfile: profileName },
    });
  }

  function createPostBody(existingPreferences = {}) {
    const postBody = {
      [CLOUD.PROFILE_NAME_PREFERENCE_PROPERTY]: profileName,
    };

    return {
      ...existingPreferences,
      ...postBody,
    };
  }

  if (namespace === SYSTEM_NAMESPACE) {
    MyPreferenceApi.getSystemPreferences().subscribe((preferences = {}) => {
      MyPreferenceApi.setSystemPreferences({}, createPostBody(preferences)).subscribe(
        successCallback,
        setError
      );
    }, setError);
  } else {
    const params = { namespace };

    MyPreferenceApi.getNamespacePreferences(params).subscribe((preferences = {}) => {
      MyPreferenceApi.setNamespacePreferences(params, createPostBody(preferences)).subscribe(
        successCallback,
        setError
      );
    });
  }
};

export const highlightNewProfile = (profileName) => {
  ProfilesStore.dispatch({
    type: PROFILES_ACTIONS.SET_NEW_PROFILE,
    payload: {
      newProfile: profileName,
    },
  });
  setTimeout(() => {
    ProfilesStore.dispatch({
      type: PROFILES_ACTIONS.SET_NEW_PROFILE,
      payload: {
        newProfile: null,
      },
    });
  }, 3000);
};

export const getNodeHours = (nodeminutes) => {
  if (typeof nodeminutes === 'number') {
    let nodeHours = nodeminutes / 60;
    return typeof nodeHours.toFixed === 'function' ? nodeHours.toFixed(2) : nodeHours;
  }
  return nodeminutes;
};
