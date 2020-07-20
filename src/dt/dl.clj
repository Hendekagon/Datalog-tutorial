(ns dt.dl
  (:require [datahike.api :as d]))

(comment
 "

  Databases like Datomic are made
  of an accumulation of facts of the
  form

  [entity attribute value]

  e.g.

  [:birds :have :wings]

  or

  [:horses :eat :carrots]

  A Datalog query is a list
  of clauses where a clause is
  a desired fact, something we want
  to be true of the results:

  [?animal :eats :carrots]

  symbols prefixed with ? are
  called variables, and they're
  what the query could return and which
  we want to be true given the rest
  of the clause -- the query engine
  will find entities that can be substituted
  for the ?variables, making the clauses true

  A query returns results for which
  *all* the clauses we list are true

  [[?animal :eats :carrots]
   [?animal :has :wings]]

  this query has two clauses and will
  return all entities which we're calling
  ?animal here but it could be anything

  [[?x :eats :carrots]
   [?x :has :wings]]

  here's one with more variables

  [[?animal ?something :carrots]
   [?animal :has :wings]]

  this vector (or list) of clauses
  is logical AND - it's like
  (and clause0 clause1 clause2...)

  the process of ensuring all clauses
  are true by making sure all
  their ?variables refer to the same
  entities is called 'unification'


  -------- footnotes --------

  Variables are just symbols -
  the ? prefix is to identify them as variables
  to the query engine

 ")


(defn make-db
  "
    Return a database value

    This is an immutable in-memory database

    In Datahike, connections and database values are
    usually interchangable, but in Datomic they're different

    Note the deref before returning the connection - d/connect
    returns an atom, for use in imperative style, but here
    I want to demonstrate pure functional style
    so I'm dereferencing it so we'll only use its value

  "
  ([]
    (make-db {:store {:backend :mem :id "mem-example"}}))
  ([cfg]
    (when (d/database-exists? cfg)
      (d/delete-database cfg))
    (d/create-database cfg)
    @(d/connect cfg)))

(defn with-schema
  "
    Return the given database
    with a schema transacted

    Note the use of d/db-with
    - this doesn't affect the database
    passed in, instead it returns a new value
    with the given data transacted, in this case
    schema data.

    It's like assoc

  "
  ([db]
    (d/db-with db
      [{:db/ident :name
         :db/valueType :db.type/keyword
         :db/unique :db.unique/identity
         :db/cardinality :db.cardinality/one}
        {:db/ident :orbits
         :db/valueType :db.type/keyword
         :db/cardinality :db.cardinality/many}
        {:db/ident :has
         :db/valueType :db.type/keyword
         :db/cardinality :db.cardinality/many}])))

(defn with-Earth-based-observations
  "
    Returns the given database
    with data observed from Earth

    Again - this is like assoc
  "
  [db]
  (d/db-with db
    [
      {:name :Saturn :orbits :Sol :has :rings}
      {:name :Jupiter :orbits :Sol :has :complex-weather}
      {:name :Titan :orbits :Saturn}
      {:name :Earth :orbits :Sol :has :lakes}
      {:name :Earth :orbits :Sol :has :complex-weather}
      {:name :Earth :orbits :Sol :has :magnetic-field}
      {:name :Earth :orbits :Sol :has :active-volcanos}
    ]))

(defn with-Saturn-probe
  "
    Returns the given database
    with data from the Saturn probe
  "
  [db]
  (d/db-with db
    [
     {:name :Titan :has :lakes}
     {:name :Rhea :orbits :Saturn :has :ice}
     {:name :Jupiter :orbits :Sol :has :red-spot}
     {:name :Jupiter :orbits :Sol :has :magnetic-field}
     {:name :Io :orbits :Jupiter :has :active-volcanos}
    ]))

(defn with-Jupiter-probe
  "
    Returns the given database
    with data from the Jupiter probe
  "
  [db]
  (d/db-with db
    [
     {:name :Jupiter :orbits :Sol :has :red-spot}
     {:name :Jupiter :orbits :Sol :has :magnetic-field}
     {:name :Io :orbits :Jupiter :has :active-volcanos}
    ]))

(defn with-Venus-probe
  "
    Returns the given database
    with data from the Venus probe
  "
  [db]
  (d/db-with db
    [
     {:name :Venus :orbits :Sol :has :complex-weather}
     {:name :Venus :orbits :Sol :has :active-volcanos}
    ]))

(defn test-query-0
  "
    Return results of an example query

    Make a database, transact a schema, add some data
    then query it

    The query has two parts:

      a :find clause, consisting of a vector of things
      we want it to return

      a :where clause, consisting of a vector of clauses
      which must all be true, which is to say that their
      variables must refer to the same things -- be 'unified'

    All the variables in :find must be a subset of
    those specified in :where

    (test-query-0)
    => ([{:name :Titan} :orbits])

    Here we used two different patterns in the :find clause,
    a pull and a direct entity...

  "
  ([]
    (->>
      (make-db)
      (with-schema)
      (with-Earth-based-observations)
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
           ]}))))

(defn test-query-1
  "
    Now let's see how immutable
    database values work

    This returns the result of querying
    two database values: the first is the one we queried
    above, the second is with more data (facts) added


  "
  ([]
   (let [db0 (->
               (make-db)
               (with-schema)
               (with-Earth-based-observations))
         db1 (with-Jupiter-probe db0)]
     (map
       (partial d/q
        '{
          :find
          [
           [pull ?celestial-body [:name :has :db/id]]
           ?celestial-body
           ]
          :where
          [
           [?celestial-body :has :magnetic-field]
           [?celestial-body :has :complex-weather]
           ]}) [db0 db1]))))

