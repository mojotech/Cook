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
(ns cook.mesos.util
  (:require [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [clj-time.core :as t]
            [clj-time.coerce :as tc]
            [datomic.api :as d :refer (q)]
            [metatransaction.core :refer (db)]
            [metrics.timers :as timers]))

(defn get-all-resource-types
  "Return a list of all supported resources types. Example, :cpus :mem :gpus ..."
  [db]
  (->> (q '[:find ?ident
            :where
            [?e :resource.type/mesos-name ?ident]]
          db)
       (map first)))

(defn categorize-job
  "Return the category of the job. Currently jobs can be :normal or :gpu. This
   is used to give separate queues for scarce & non-scarce resources"
  [job]
  (let [resources (:job/resource job)]
    (if (some #(= :resource.type/gpus (:resource/type %)) resources)
      :gpu
      :normal)))

(defn without-ns
  [k]
  (if (keyword? k)
    (keyword (name k))
    k))

;; These two walk functions were copied from https://github.com/clojure/clojure/blob/master/src/clj/clojure/walk.clj
;; because the line about datomic.query.EntityMap needed to be added..
(defn walk
  "Traverses form, an arbitrary data structure.  inner and outer are
  functions.  Applies inner to each element of form, building up a
  data structure of the same type, then applies outer to the result.
  Recognizes all Clojure data structures. Consumes seqs as with doall."

  {:added "1.1"}
  [inner outer form]
  (cond
   (list? form) (outer (apply list (map inner form)))
   (instance? clojure.lang.IMapEntry form) (outer (vec (map inner form)))
   (seq? form) (outer (doall (map inner form)))
   (instance? clojure.lang.IRecord form)
     (outer (reduce (fn [r x] (conj r (inner x))) form form))
   ;; Added the line below to work with datomic entities..
   (instance? datomic.query.EntityMap form) (outer (into {} (map inner) form))
   (coll? form) (outer (into (empty form) (map inner) form))
   :else (outer form)))

(defn postwalk
  "Performs a depth-first, post-order traversal of form.  Calls f on
  each sub-form, uses f's return value in place of the original.
  Recognizes all Clojure data structures. Consumes seqs as with doall."
  {:added "1.1"}
  [f form]
  (walk (partial postwalk f) f form))

(defn deep-transduce-kv
  "Recursively applies the transducer xf over all kvs in the map or
  any nested maps"
  [xf m]
  (postwalk (fn [x]
              (if (map? x)
                (into {} xf x)
                x))
            m))

(defn remove-datomic-namespacing
  "Takes a map from datomic (pull) and removes the namespace
   as well as :db/id keys"
  [datomic-map]
  (->> datomic-map
       (deep-transduce-kv (comp
                            (filter (comp (partial not= :db/id) first))
                            (map (fn [[k v]]
                                   ;; This if is here in the case when a ident is used as
                                   ;; an enum and the data is gotten from the pull api.
                                   ;; It will be represented as:
                                   ;; {:thing/type {:ident :ident/thing}}
                                   (if (and (map? v) (:ident v))
                                     [k (without-ns (:ident v))]
                                     [k v])))
                            (map (juxt (comp without-ns first) second))))
       ;; Merge with {} in case datomic-map was nil so we get empty map back
       (merge {})))

(defn job-ent->container
  "Take a job entity and return its container"
  [db job-ent]
  (when-let [ceid (:db/id (:job/container job-ent))]
    (->> (d/pull db "[*]" ceid)
         remove-datomic-namespacing)))

(defn job-ent->env
  "Take a job entity and return the environment variable map"
  [job-ent]
  (reduce (fn [m env-var]
            (assoc m
                   (:environment/name env-var)
                   (:environment/value env-var)))
          {}
          (:job/environment job-ent)))

(defn job-ent->label
  "Take a job entity and return the label map"
  [job-ent]
  (reduce (fn [m label-var]
            (assoc m
                   (:label/key label-var)
                   (:label/value label-var)))
          {}
          (:job/label job-ent)))

(defn job-ent->resources
  "Take a job entity and return a resource map. NOTE: the keys must be same as mesos resource keys"
  [job-ent]
  (reduce (fn [m r]
            (let [resource (keyword (name (:resource/type r)))]
              (condp contains? resource
                #{:cpus :mem :gpus} (assoc m resource (:resource/amount r))
                #{:uri} (update-in m [:uris] (fnil conj [])
                                   {:cache (:resource.uri/cache? r false)
                                    :executable (:resource.uri/executable? r false)
                                    :value (:resource.uri/value r)
                                    :extract (:resource.uri/extract? r false)}))))
          {:ports (let [ports (:job/ports job-ent 0)]
                    (if (:job/custom-executor job-ent true)
                      ports
                      ;; XXX: The cook executor needs a port for its web API
                      ;; Rather than adding a port at write time, which could
                      ;; break bacwkard compatability for jobs created with the
                      ;; datomic API, add a port at read time. That being said,
                      ;; this feels a little dirty.
                      (+ ports 1)))}
          (:job/resource job-ent)))

(defn job-ent->attempts-consumed
  "Determines the amount of attempts consumed by a job-ent."
  [db job-ent]
  (d/invoke db :job/attempts-consumed db job-ent))

(defn command-ents->commands
  [command-ents suffix]
  (->> command-ents
       (sort-by :command/order)
       (map
        (fn [c]
          {:name (str suffix "." (:command/order c))
           :value (:command/value c)
           :async (:command/async? c false)
           :guard (:command/guard? c false)}))))

(defn job-ent->before-commands
  [job-ent]
  (command-ents->commands
   (:job/before-command job-ent) "before"))

(defn job-ent->after-commands
  [job-ent]
  (command-ents->commands
   (:job/after-command job-ent) "after"))

(defn job-ent->commands
  [job-ent]
  (concat
   (job-ent->before-commands job-ent)
   [{:value (:job/command job-ent)
     :async false
     :guard false}]
   (job-ent->after-commands job-ent)))

(defn code-ents->codes
  [code-ents]
  (->> code-ents (sort-by :code/order) (map :code/value)))

(defn instance-ent->before-codes
  [instance-ent]
  (-> instance-ent :instance/before-code code-ents->codes))

(defn instance-ent->after-codes
  [instance-ent]
  (-> instance-ent :instance/after-code code-ents->codes))

(defn sum-resources-of-jobs
  "Given a collections of job entities, returns the total resources they use
   {:cpus cpu :mem mem}"
  [job-ents]
  (loop [total-cpus 0.0
         total-mem 0.0
         [job-ent & job-ents] job-ents]
    (if job-ent
      (let [{:keys [cpus mem]} (job-ent->resources job-ent)]
        (recur (+ total-cpus (or cpus 0))
               (+ total-mem (or mem 0))
               job-ents))
      {:cpus total-cpus :mem total-mem})))

(timers/deftimer [cook-mesos scheduler get-pending-jobs-duration])

(defn get-pending-job-ents
  ([filtered-db]
   (get-pending-job-ents filtered-db filtered-db))
  ([filtered-db unfiltered-db]
   (timers/time!
     get-pending-jobs-duration
     ;; This function can use an unfiltered db when creating entities
     ;; because a job will either be fully committed or fully not committed,
     ;; therefore if the :job/state attribute was committed the full entity
     ;; is committed and we need not worry about checking commit status
     (->> (q '[:find ?j
               :in $ [?state ...]
               :where
               [?j :job/state ?state]]
             filtered-db [:job.state/waiting])
          (map (fn [[x]] (d/entity unfiltered-db x)))))))

(defn get-jobs-by-user-and-state
  "Returns all job entities for a particular user
   in a particular state."
  [db user state start end]
  (->> (if (= state :job.state/completed)
         ;; Datomic query performance is based entirely on the size of the set
         ;; of the first where clause. In the case of :job.state/completed
         ;; the total number of jobs for a user should be smaller than
         ;; all completed jobs by any user i.e.
         ;; (waiting_user + running_user < completed - completed_user)
         (q '[:find [?j ...]
              :in $ ?user ?state ?start ?end
              :where
              [?j :job/user ?user]
              [?j :job/state ?state]
              [?j :job/submit-time ?t]
              [(< ?start ?t)]
              [(< ?t ?end)]
              [?j :job/custom-executor false]]
            db user state start end)
         (q '[:find [?j ...]
              :in $ ?user ?state ?start ?end
              :where
              [?j :job/state ?state]
              [?j :job/user ?user]
              [?j :job/submit-time ?t]
              [(< ?start ?t)]
              [(< ?t ?end)]
              [?j :job/custom-executor false]]
            db user state start end))
       (map (partial d/entity db))))

(timers/deftimer [cook-mesos scheduler get-running-tasks-duration])

(defn get-running-task-ents
  "Returns all running task entities"
  [db]
  (timers/time!
    get-running-tasks-duration
    (->> (q '[:find ?i
              :in $ [?status ...]
              :where
              [?i :instance/status ?status]]
            db [:instance.status/running :instance.status/unknown])
         (map (fn [[x]] (d/entity db x))))))

(defn job-allowed-to-start?
  "Converts the DB function :job/allowed-to-start? into a predicate"
  [db job]
  (try
    (d/invoke db :job/allowed-to-start? db (:db/id job))
    true
    (catch clojure.lang.ExceptionInfo e
      false)))

(defn create-task-ent
  "Takes a pending job entity and returns a synthetic running task entity for that job"
  [pending-job-ent & {:keys [hostname] :or {hostname nil}}]
  {:job/_instance pending-job-ent
   :instance/status :instance.status/running
   :instance/hostname hostname})

(defn task-ent->user
  [task-ent]
  (get-in task-ent [:job/_instance :job/user]))

(def ^:const default-job-priority 50)


(defn task->feature-vector
  [task]
  "Vector of comparable features of a task.
   Last two elements are aribitary tie breakers.
   Use :db/id because they guarantee uniqueness for different entities
   (:db/id task) is not sufficient because synthetic task entities don't have :db/id
    This assumes there are at most one synthetic task for a job, otherwise uniqueness invariant will break"
  [(- (:job/priority (:job/_instance task) default-job-priority))
   (:instance/start-time task (java.util.Date. Long/MAX_VALUE))
   (:db/id task)
   (:db/id (:job/_instance task))])

(defn same-user-task-comparator
  "Comparator to order same user's tasks"
  []
  (fn [task1 task2]
      (compare (task->feature-vector task1) (task->feature-vector task2))))

(defn retry-job!
  "Sets :job/max-retries to the given value for the given job UUID.
   Throws an exception if there is no job with that UUID."
  [conn uuid retries]
  (try
    (let [eid (-> (d/entity (d/db conn) [:job/uuid uuid])
                  :db/id)]
      @(d/transact conn
                   [[:db/add [:job/uuid uuid]
                     :job/max-retries retries]

                    ;; If the job is in the "completed" state, put it back into
                    ;; "waiting":
                    [:db.fn/cas [:job/uuid uuid]
                     :job/state (d/entid (d/db conn) :job.state/completed) :job.state/waiting]]))
    ;; :db.fn/cas throws an exception if the job is not already in the "completed" state.
    ;; If that happens, that's fine. We just set "retries" only and continue.
    (catch java.util.concurrent.ExecutionException e
      (if-not (.startsWith (.getMessage e)
                           "java.lang.IllegalStateException: :db.error/cas-failed Compare failed:")
        (throw (ex-info "Exception while retrying job" {:uuid uuid :retries retries} e))
        @(d/transact conn
                     [[:db/add [:job/uuid uuid]
                       :job/max-retries retries]])))))

(defn filter-sequential
  "This function allows for filtering when the filter function needs to consider previous elements
   Lazily filters elements of coll.
   f is assumed to take two parameters, state and an element, i.e. (f state element)
   and return a pair [new-state should-keep?] where new-state will be passed to f when called on the next element.
   The new-state is passed regardless of whether should-keep? is truth-y or not."
  [f init-state coll]
  (letfn [(fr [{:keys [state]} x]
            (let [[state' should-keep?] (f state x)]
              {:state state'
               :x (when should-keep? x)}))]
    (->> coll
      (reductions fr {:state init-state :x nil})
      (filter :x)
      (map :x))))

(defn task-run-time
  "Returns the run time of the task as a joda interval"
  [task-ent]
  (let [start (tc/from-date (:instance/start-time task-ent))
        end (or (tc/from-date (:instance/end-time task-ent))
                (t/now))]
    (t/interval start end)))

(defn namespace-datomic
  "Namespaces keywords given the datomic conventions

   Examples:
   (namespace-datomic :straggler-handling :type)
   :straggler-handling/type
   (namespace-datomic :straggler-handling :type :quantile-deviation)
   :straggler-handling.type/quantile-deviation"
  ([name-space value]
   (keyword (name name-space) (name value)))
  ([name-space subspace value]
   (namespace-datomic (str (name name-space) "." (name subspace)) value)))
