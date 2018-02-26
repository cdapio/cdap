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

package co.cask.cdap.test.base;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.AbstractService;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class IdleTest {
  static class IdleService extends AbstractIdleService {

    @Override
    protected void startUp() throws Exception {
      TimeUnit.SECONDS.sleep(2);
      System.out.println("startUp finished");
    }

    @Override
    protected void shutDown() throws Exception {

    }
  }

  static class NewService extends AbstractService {
    @Override
    protected void doStart() {
      Executors.newSingleThreadExecutor().submit(new Runnable() {
        @Override
        public void run() {
          try {
            TimeUnit.SECONDS.sleep(2);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          System.out.println("starting");
          try {
            TimeUnit.SECONDS.sleep(2);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          notifyStarted();
          System.out.println("started notified");
          try {
            TimeUnit.SECONDS.sleep(2);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          System.out.println("startup DONE");
        }
      });
    }

    @Override
    protected void doStop() {
      Executors.newSingleThreadExecutor().submit(new Runnable() {
        @Override
        public void run() {
          try {
            TimeUnit.SECONDS.sleep(2);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          System.out.println("stopping");
          try {
            TimeUnit.SECONDS.sleep(2);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          notifyStopped();
          System.out.println("stopped notified");
          try {
            TimeUnit.SECONDS.sleep(2);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          System.out.println("stop DONE");
        }
      });

    }
  }

  public static void main(String[] args) {
    NewService newService = new NewService();
    newService.startAndWait();
    System.out.println("startAndWAIT DONE");
    newService.stopAndWait();
    System.out.println("stopAndWAIT DONE");
  }
}
