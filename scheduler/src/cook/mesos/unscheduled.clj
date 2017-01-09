;;
;; Copyright (c) Two Sigma Open Source, LLC
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;  http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
(ns cook.mesos.unscheduled
  (:require [clojure.tools.logging :as log]
            [datomic.api :as d :refer (q)]
            [cook.mesos.scheduler :as scheduler]
            [cook.mesos.quota :as quota]
            [cook.mesos.share :as share]
            [cook.mesos.util :as util]
            [clojure.edn :as edn])
  (import java.util.Date))

(defn check-exhausted-retries
  [job]
  (let [max-retries (:job/max-retries job)
        instance-count (count (:job/instance job))]
    (if (>= instance-count max-retries)
      ["Job has exhausted its maximum number of retries."
       {:max-retries max-retries
        :instance-count instance-count}])))


(defn how-job-would-exceed-resource-limits
  [limits running-jobs job]
  (let [jobs-with-new (conj running-jobs job)
        usages (map scheduler/job->usage jobs-with-new)
        total-usage (reduce (partial merge-with +) usages)]
    (->> (map (fn [[k v]]
                (when (> (or (k total-usage) 0) v)
                  [k {:limit v :usage (k total-usage)}]))
              limits)
         (filter seq)
         (into {}))))

(defn check-exceeds-limit
  [read-limit-fn err-msg db job]
  (when (= (:job/state job) :job.state/waiting)
    (let [user (:job/user job)
          ways (how-job-would-exceed-resource-limits
                (read-limit-fn db user)
                (util/get-jobs-by-user-and-state db user :job.state/running
                                                 (Date. 0) (Date.))
                job)]
      (if (seq ways)
        [err-msg ways]
        nil))))

(defn reasons
  [conn job]
  (let [db (d/db conn)]
    (case (:job/state job)
      :job.state/running [["The job is running now." {}]]
      :job.state/completed [["The job already completed." {}]]
      (filter some?
              [(check-exhausted-retries job)
               (check-exceeds-limit quota/get-quota
                                    "The job would cause you to exceed resource quotas."
                                    db job)
               (check-exceeds-limit share/get-share
                                    "The job would cause you to exceed resource shares."
                                    db job)]))))

