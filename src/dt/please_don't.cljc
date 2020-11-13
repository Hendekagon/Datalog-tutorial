(ns dt.please-don't
  (:require [datahike.api :as d]))


(comment
"
  Only read this if you're from
  an imperative background like
  Python or Java, otherwise stop
  reading
")


(comment
"
  If you're from a background

")

(defn get-conn
  "
    Return a mutable database
    connection
  "
  ([]
    (get-conn {:store {:backend :mem :id "mem-example"}}))
  ([cfg]
    (when (d/database-exists? cfg)
      (d/delete-database cfg))
    (d/create-database cfg)
    (d/connect cfg)))

(defn schema
  "
    Return a schema to be transacted
  "
  ([]
   [{:db/ident :name
     :db/valueType :db.type/keyword
     :db/unique :db.unique/identity
     :db/cardinality :db.cardinality/one}
    {:db/ident :orbits
     :db/valueType :db.type/keyword
     :db/cardinality :db.cardinality/many}
    {:db/ident :has
     :db/valueType :db.type/keyword
     :db/cardinality :db.cardinality/many}]))

(defn Earth-based-observations
  "
    Returns data observed from Earth
  "
  []
  [
   {:name :Sol}
   {:name :Saturn :orbits :Sol :has :rings}
   {:name :Jupiter :orbits :Sol :has :complex-weather}
   {:name :Titan :orbits :Saturn}
   {:name :Earth :orbits :Sol :has :lakes}
   {:name :Earth :orbits :Sol :has :complex-weather}
   {:name :Earth :orbits :Sol :has :magnetic-field}
   {:name :Earth :orbits :Sol :has :active-volcanos}
   ])

(defn Saturn-probe
  "
    Returns data from the Saturn probe
  "
  []
  [
   {:name :Titan :has :lakes}
   {:name :Rhea :orbits :Saturn :has :ice}
   ])

(defn test-example-0a [{conn :conn}]
  (d/transact conn (schema))
  (d/transact conn (Earth-based-observations))
  (d/transact conn (Saturn-probe))
  (d/q
    '{
      :find
      [
       [pull ?celestial-body [:name]]
       ?relationship-to
       ]
      :where
      [
       [?celestial-body ?relationship-to :Saturn]
       [?celestial-body :has :lakes]
       ]} (d/db conn)))

(defn test-example-0b [{conn :conn}]
  (d/transact conn (schema))
  (d/transact conn (Earth-based-observations))
  (d/q
    '{
      :find
      [
       [pull ?celestial-body [:name]]
       ?relationship-to
       ]
      :where
      [
       [?celestial-body ?relationship-to :Saturn]
       [?celestial-body :has :lakes]
       ]} (d/db conn)))

(comment
  (let [params {:conn (get-conn)}]
    {:0a (test-example-0a params)
     :0b (test-example-0b params)})
  )