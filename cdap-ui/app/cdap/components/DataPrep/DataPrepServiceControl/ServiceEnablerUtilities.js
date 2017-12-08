/*
 * Copyright © 2017 Cask Data, Inc.
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

import NamespaceStore from 'services/NamespaceStore';
import {findHighestVersion} from 'services/VersionRange/VersionUtilities';
import {MyArtifactApi} from 'api/artifact';
import Version from 'services/VersionRange/Version';
import T from 'i18n-react';
import {Subject} from 'rxjs/Subject';

export default function enableDataPreparationService({
  shouldStopService,
  artifactName,
  api,
  i18nPrefix,
  MIN_VERSION
}) {
  function enableService(observer) {

    /**
     *  1. Get Wrangler Service App
     *  2. If not found, create app
     *  3. Start Wrangler Service
     *  4. Poll until service starts, then reload page
     **/

    let namespace = NamespaceStore.getState().selectedNamespace;

    MyArtifactApi.list({ namespace })
      .subscribe((res) => {
        let appArtifact = res.filter((artifact) => {
          return artifact.name === artifactName;
        });

        if (appArtifact.length === 0) {
          observer.error({
            error: T.translate('features.ServiceEnableUtility.serviceNotFound', {artifactName})
          });
          return;
        }

        let versionsArray = appArtifact.map((artifact) => {
          return artifact.version;
        });

        let highestVersion = findHighestVersion(versionsArray, true);

        if (MIN_VERSION) {
          let minimumVersion = new Version(MIN_VERSION);

          if (minimumVersion.compareTo(new Version(highestVersion)) > 0) {
            observer.error({
              error: T.translate(`${i18nPrefix}.minimumVersionError`, {
                highestVersion,
                minimumVersion: MIN_VERSION
              })
            });
            return;
          }
        }


        let highestVersionArtifact = appArtifact.filter((artifact) => {
          return artifact.version === highestVersion;
        });

        if (highestVersionArtifact.length > 1) {
          // The only times when the length is greater than 1 is
          // when there are two artifacts with the same version;
          // USER and SYSTEM scope. In that case, take the USER scope
          highestVersionArtifact = highestVersionArtifact[0];
          highestVersionArtifact.scope = 'USER';
        } else {
          highestVersionArtifact = highestVersionArtifact[0];
        }

        api.getApp({ namespace })
          .subscribe((res) => {
            if (res.artifact.version !== highestVersion) {
              // there's higher version available, so create
              // new app with the higher version
              createApp(highestVersionArtifact, observer);
              return;
            }

            // Wrangler app already exist
            // Just start service
            startService(observer);
          }, () => {
            // App does not exist
            // Go to create app
            createApp(highestVersionArtifact, observer);
          });
      });

  }

  function createApp(artifact, observer) {
    let namespace = NamespaceStore.getState().selectedNamespace;

    api.createApp({ namespace }, { artifact })
      .subscribe(() => {
        startService(observer);
      }, (err) => {
        observer.error({
          error: 'Failed to enable data preparation',
          extendedMessage: err.data || err
        });
      });
  }

  function startService(observer) {
    let namespace = NamespaceStore.getState().selectedNamespace;

    api.startService({ namespace })
      .subscribe(() => {
        pollServiceStatus(observer);
      }, (err) => {
        observer.error({
          error: 'Failed to enable data preparation',
          extendedMessage: err.data || err
        });
      });
  }

  function pollServiceStatus(observer) {
    let namespace = NamespaceStore.getState().selectedNamespace;

    let servicePoll = api.pollServiceStatus({ namespace })
      .subscribe((res) => {
        if (res.status === 'RUNNING') {
          servicePoll.unsubscribe();
          pingService(observer);
        }
      }, (err) => {
        observer.error({
          error: 'Failed to enable data preparation',
          extendedMessage: err.data || err
        });
      });
  }

  function pingService(observer) {
    let namespace = NamespaceStore.getState().selectedNamespace;

    function ping() {
      api.ping({ namespace })
        .subscribe(() => {
          observer.next();
        }, (err) => {
          if (err.statusCode === 503) {
            setTimeout(() => {
              ping();
            }, 2000);
            return;
          }

          observer.error({
            error: 'Error while communicating with Data Preparation Service',
            extendedMessage: err.data || err
          });
        });
    }

    ping();
  }

  function stopService(observer) {
    let namespace = NamespaceStore.getState().selectedNamespace;

    api.stopService({ namespace })
      .subscribe(() => {
        pollStopServiceStatus(observer);
      }, (err) => {
        observer.error({
          error: 'Failed to stop Data Preparation service',
          extendedMessage: err.data || err
        });
      });

  }

  function pollStopServiceStatus(observer) {
    let namespace = NamespaceStore.getState().selectedNamespace;

    let servicePoll = api.pollServiceStatus({ namespace })
      .subscribe((res) => {
        if (res.status === 'STOPPED') {
          enableService(observer);

          servicePoll.unsubscribe();
        }
      }, (err) => {
        observer.error({
          error: 'Failed to stop Data Preparation service',
          extendedMessage: err.data || err
        });
      });
  }


  let subject = new Subject();

  if (shouldStopService) {
    stopService(subject);
  } else {
    enableService(subject);
  }

  return subject;
}
