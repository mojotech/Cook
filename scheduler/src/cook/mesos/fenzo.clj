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
(ns cook.mesos.fenzo
  (:require [clojure.tools.logging :as log]
            [datomic.api :as d])
  (import java.util.concurrent.TimeUnit
          org.apache.mesos.Protos$Offer
          com.netflix.fenzo.TaskAssignmentResult
          com.netflix.fenzo.TaskScheduler
          com.netflix.fenzo.VirtualMachineLease
          com.netflix.fenzo.plugins.BinPackingFitnessCalculators))

(defn count-resource-failure
  [totals resource-failure]
  (update-in totals [:resources (str (.getResource resource-failure))] (fnil inc 0)))

(defn summarize-placement-failure
  [totals assignment-result]
  (let [resources-counted (reduce count-resource-failure totals (.getFailures assignment-result))
        constraint-failure (.getConstraintFailure assignment-result)]
    (if constraint-failure
      (update-in resources-counted [:constraints (.getName constraint-failure)] (fnil inc 0))
      resources-counted)))

(defn accumulate-placement-failures
  "Param should be a collection of Fenzo placement failure results for a single
  job.  Returns a tuple of [job id, single string containing all of the result
  messages].  Along the way, logs each result message if debug-level logging
  is enabled."
  [failures]
  (let [job (:job  (.. (first failures) (getRequest)))
        under-investigation? (:job/under-investigation job)
        summary (when (or under-investigation? (log/enabled? :debug))
                  (reduce summarize-placement-failure {} failures))]
    (when (log/enabled? :debug)
      (log/debug "Job" (:job/uuid job) " placement failure summary:" summary))
    (when (and under-investigation? (seq summary))
      [(:db/id job) summary])))

(defn record-placement-failures!
  "For any failure-results for jobs that are under investigation,  persists all
  Fenzo placement errors as an attribute of the job.  Also logs information about
  placement errors if debug-level logging is enabled."
  [conn failure-results]
  (log/debug "Fenzo placement failure information:" failure-results)
  (let [failures-by-job (into {} (map accumulate-placement-failures
                                      failure-results))
        transactions (mapv (fn [[job-db-id failures-description]]
                             {:db/id job-db-id
                              :job/under-investigation false
                              :job/last-fenzo-placement-failure (str failures-description)})
                           failures-by-job)]
    (when (seq transactions)
      @(d/transact conn transactions))))

