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
import {MyMetricApi} from 'api/metric';
import {MySearchApi} from 'api/search';
import {GLOBALS} from 'services/global-constants';

export const getProfileMetricsBody = (queryId, namespace, profileScope, startTime, endTime, extraTags) => {
  let metricBody = {
    [queryId]: {
      tags: {
        namespace,
        profilescope: profileScope
      },
      metrics: [
        'system.program.completed.runs',
        'system.program.node.minutes'
      ],
      timeRange: {
        start: startTime,
        end: endTime,
        resolution: "auto"
      },
      groupBy: ['profile']
    }
  };
  if (extraTags) {
    metricBody = {
      ...metricBody,
      [queryId]: {
        ...metricBody[queryId],
        tags: {
          ...metricBody[queryId].tags,
          ...extraTags
        }
      }
    };
  }
  if (!startTime && !endTime) {
    metricBody = {
      ...metricBody,
      [queryId]: {
        ...metricBody[queryId],
        timeRange: {
          ...metricBody[queryId].timeRange,
          resolution: "1h",
          aggregate: true
        }
      }
    };
  }
  return metricBody;
};

const convertMetadataToAssociations = (metadata) => {
  let schedulesCount = 0, triggersCount = 0;
  metadata.forEach(m => {
    if (m.entityId.schedule) {
      // fixed name for time based schedule.
      if (m.entityId.schedule === GLOBALS.defaultScheduleId) {
        schedulesCount += 1;
      } else {
        triggersCount += 1;
      }
    }
  });
  return {schedulesCount, triggersCount};
};

const updateScheduleAndTriggersToStore = (profileName, metadata) => {
  let {schedulesCount, triggersCount} = convertMetadataToAssociations(metadata);
  ProfilesStore.dispatch({
    type: PROFILES_ACTIONS.SET_SCHEDULES_TRIGGERS_COUNT,
    payload: {
      profile: profileName,
      schedulesCount,
      triggersCount
    }
  });
};
export const ONEDAYMETRICKEY = 'oneDayMetrics';
export const OVERALLMETRICKEY = 'overAllMetrics';
export const fetchAggregateProfileMetrics = (namespace, profile, extraTags) => {
  let oneDayMetricsRequestBody = getProfileMetricsBody(ONEDAYMETRICKEY, namespace, profile.scope, 'now-24h', 'now', extraTags);
  let overAllMetricsRequestBody = getProfileMetricsBody(OVERALLMETRICKEY, namespace, profile.scope, 0, 0, extraTags);
  return MyMetricApi
    .query(null, { ...oneDayMetricsRequestBody, ...overAllMetricsRequestBody })
    .flatMap(
      (metrics) => {
        let metricsMap = {
          [ONEDAYMETRICKEY]: {
            runs: '--',
            minutes: '--'
          },
          [OVERALLMETRICKEY]: {
            runs: '--',
            minutes: '--'
          }
        };
        Object.keys(metrics).forEach(query => {
          metrics[query].series.forEach(metric => {
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
            metricsMap[query]= {
              ...metricsMap[query],
              [metricName]: metricValue
            };
          });
        });
        return Observable.create(observer => {
          observer.next(metricsMap);
        });
      }
    );
};

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
        let profiles = namespaceProfiles
          .concat(systemProfiles)
          .map(profile => ({...profile, oneDayMetrics: {}, overAllMetrics: {}, schedulesCount: '--', triggersCount: '--'}));
        ProfilesStore.dispatch({
          type: PROFILES_ACTIONS.SET_PROFILES,
          payload: { profiles }
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

        let oneDayUSERMetricsBody = getProfileMetricsBody('oneDayUSERMetrics', namespace, 'USER', 'now-24h', 'now');
        let overAllUSERMetricsBody = getProfileMetricsBody('overAllUSERMetrics', namespace, 'USER', 0, 0);
        let oneDaySYSTEMMetricsBody = getProfileMetricsBody('oneDaySYSTEMMetrics', namespace, 'SYSTEM', 'now-24h', 'now');
        let overAllSYSTEMMetricsBody = getProfileMetricsBody('overAllSYSTEMMetrics', namespace, 'SYSTEM', 0, 0);
        MyMetricApi
          .query(null, {...oneDayUSERMetricsBody, ...overAllUSERMetricsBody, ...oneDaySYSTEMMetricsBody, ...overAllSYSTEMMetricsBody})
          .subscribe(metrics => {
            let profilesToMetricsMap = {};
            Object.keys(metrics).forEach(query => {
              // oneDayMetrics, overMetrics are the keys for metrics for each profile.
              let metricsKey = query.replace(/USER|SYSTEM/, '');
              metrics[query].series.forEach(metric => {
                let profileName = extractProfileName(metric.grouping.profile);
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
                if (!profilesToMetricsMap.hasOwnProperty(profileName)) {
                    profilesToMetricsMap[profileName] = {[metricsKey]: {}};
                }
                /*
                  {
                    profile1: {
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
                profilesToMetricsMap[profileName] = {
                  ...profilesToMetricsMap[profileName],
                  [metricsKey]: { ...profilesToMetricsMap[profileName][metricsKey], [metricName]: metricValue }
                };
              });
            });
            ProfilesStore.dispatch({
              type: PROFILES_ACTIONS.SET_PROFILE_METRICS,
              payload: {
                profilesToMetricsMap
              }
            });
          });
        profiles.forEach(profile => {
          let {scope} = profile;
          scope = scope.toLowerCase();
          let profileName = `profile:${scope}:${profile.name}`;
          let apiObservable$;
          if (namespace === 'system') {
            apiObservable$ = MySearchApi.searchSystem({ query: profileName });
          } else {
            apiObservable$ = MySearchApi.search({ namespace, query: profileName });
          }
          apiObservable$
            .subscribe(res => updateScheduleAndTriggersToStore(profile.name, res.results));
        });
      },
      setError
    );
};

export const exportProfile = (namespace, profile) => {
  let apiObservable$ = MyCloudApi.get({ namespace, profile: profile.name });
  if (namespace === 'system') {
    apiObservable$ = MyCloudApi.getSystemProfile({ profile: profile.name });
  }
  apiObservable$
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
    let apiObservable$ = MyCloudApi.create({
      namespace,
      profile: jsonSpec.name
    }, jsonSpec);
    if (namespace === 'system') {
      apiObservable$ = MyCloudApi.createSystemProfile({
        profile: jsonSpec.name
      }, jsonSpec);
    }
    apiObservable$
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
