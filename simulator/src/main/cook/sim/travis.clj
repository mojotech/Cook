(ns cook.sim.travis
  (:require [clojure.set :refer [intersection]]
            [clojure.string :as string]
            [clojure.tools.cli :refer [parse-opts]]
            [clj-http.client :as http]
            [cook.sim.database :as db]
            [cook.sim.schedule :as schedule]
            [cook.sim.runner :as runner]
            [cook.sim.reporting :as reporting]
            [cook.sim.system :as sys]
            [com.stuartsierra.component :as component]
            [robert.bruce :refer [try-try-again]]))


(defn wait-for-cook
  [settings]
  (try-try-again {:sleep 5000 :tries 20
                  :error-hook #(println "Cook API not up yet:" (.getMessage %))}
                 ;; will still throw exception on connection refused.
                 http/get (:cook-api-uri settings) {:throw-exceptions false}))

(defn schedulable?
  "Returns true iff the job is intended to be schedulable according to the
  configuration of the Minimesos cluster on travis.  We're accomplishing this
  by means of a usage profile and and minimesos config that we know to be incompatible
  - see minimesosFile and simulator_config.clj"
  [job]
  (not (string/starts-with? (:username job) "unsched")))

(defn sim-progress
  [sim-db cook-db sim-id]
  (let [jobs (reporting/job-results-from-components sim-db cook-db sim-id)
        [schedulable-jobs unschedulable-jobs] ((juxt filter remove) schedulable? jobs)
        [scheduled-jobs unscheduled-jobs] ((juxt filter remove) :wait-time jobs)
        [finished-jobs unfinished-jobs] ((juxt filter remove) :turnaround jobs)]
    {:schedulable {:total (count schedulable-jobs)
                   :unscheduled (count (intersection (set schedulable-jobs) (set unscheduled-jobs)))
                   :unfinished (count (intersection (set schedulable-jobs) (set unfinished-jobs)))}
     :unschedulable {:total (count unschedulable-jobs)
                     :unscheduled (count (intersection (set unschedulable-jobs) (set unscheduled-jobs)))
                     :unfinished (count (intersection (set unschedulable-jobs) (set unscheduled-jobs)))}}))

(defn sim-finished?
  [sim-db cook-db sim-id]
  (let [progress (sim-progress sim-db cook-db sim-id)
        count-unscheduled (-> progress :schedulable :unscheduled)
        count-unfinished (-> progress :schedulable :unfinished)]
    (println count-unscheduled "unscheduled schedulable jobs.")
    (println count-unfinished "unfinished schedulable jobs.")
    (if (zero? count-unfinished) progress false)))

(defn wait-for-sim-to-finish
  [sim-db cook-db sim-id timeout-seconds]
  (let [sleep-seconds 5]
    (try-try-again {:sleep (* 1000 sleep-seconds)
                    :tries (/ timeout-seconds sleep-seconds)
                    :return? :truthy?
                    :error-hook #(println "Sim not finished yet." %)}
                   sim-finished? sim-db cook-db sim-id)))


(defn perform-ci
  [settings sim-db cook-db]
  (let [timeout-secs (* 5 60)
        file "travis-schedule.edn"
        _ (schedule/generate-job-schedule! settings file)
        schedule-id (schedule/import-schedule! sim-db file)
        _ (wait-for-cook settings)
        sim-id (runner/simulate! settings sim-db schedule-id "Travis run")
        final-progress (wait-for-sim-to-finish sim-db cook-db sim-id timeout-secs)
        unschedulable-jobs (:unschedulable final-progress)
        num-scheduled-unschedulable (- (:total unschedulable-jobs) (:unscheduled unschedulable-jobs))]
    (reporting/analyze settings sim-db cook-db sim-id)
    (if (not final-progress)
      (throw (Exception. "Sim never finished.")))
    (if (zero? num-scheduled-unschedulable)
      (println (:total unschedulable-jobs) " intentionally unschedulable jobs were never scheduled.  Perfect!")
      (throw (Exception. (str num-scheduled-unschedulable " supposedly unschedulable jobs were scheduled."))))))
