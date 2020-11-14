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
  what the query could return as results.
  The query engine will find entities
  that can be substituted for the ?variables,
  making the clauses true

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

  In the example above, we want entities
  ?animal which both :eats :carrots and
  :has :wings

  Entities are stored as numbers
  in the database


  -------- notes --------

  Variables are just symbols -
  the ? prefix is to identify them as variables
  to the query engine

  Evaluate the test-query-x functions,
  modify them and see what happens

  -----------------------
 ")


(defn make-db
  "
    Return a an immutable in-memory database value

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

(defn get-schema
  "
    Return the given database
    with a schema transacted

    Note the use of d/db-with
    - this doesn't affect the database
    passed in, instead it returns a new value
    with the given data transacted, in this case
    schema data.

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

(defn Saturn-probe-observations
  "
    Returns data from the Saturn probe
  "
  []
  [
   {:name :Titan :has :lakes}
   {:name :Rhea :orbits :Saturn :has :ice}
   ])

(defn Jupiter-probe-observations
  "
    Returns data from the Jupiter probe
  "
  []
  [
   {:name :Jupiter :has #{:magnetic-field :red-spot}}
   {:name :Io :orbits :Jupiter :has :active-volcanos}
   ])

(defn Venus-probe-observations
  "
    Returns data from the Venus probe
  "
  []
  [
   {:name :Venus :has :complex-weather}
   {:name :Venus :orbits :Sol :has :active-volcanos}
   ])

(defn
  ^{:doc
    [:div
     [:h1 "Return results of an example query"]
     [:div "Make a database, transact a schema, add some data
            then query"]
     [:ul "The query has two parts:"
      [:li [:span "a" [:span.ref {:ref [:find]} [:code ":find"] [:span "clause"]] [:span ", consisting of a vector of things
              we want it to return"]]]
      [:li "a :where clause, consisting of a vector of clauses
              which must all be true, which is to say that their
              variables must refer to the same things -- be 'unified'"]]
     [:p "All the variables in :find must be a subset of
            those specified in :where"]
     [:code
      "{:in (test-query-0)
        :out ([{:name :Titan} :orbits])}"]
     [:p "Here we used two different patterns in the :find clause,
          a pull and a direct entity..."]]}
  test-query-0
  ([]
    (->
      (make-db)
      (d/with (get-schema))
      :db-after
      (d/with (Earth-based-observations))
      :db-after
      (d/with (Saturn-probe-observations))
      :db-after
      (->>
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
            ]})))))

(comment
  (test-query-0))

(defn test-query-1
  "
    Now let's see how immutable
    database values work

    This returns the result of querying
    two database values: first with only Earth-based
    observations, second with added data from
    a Jupiter probe

    We get 2 result lists, the second of which
    includes the data we added -- and look, the
    first result shows that the first database
    value db0 is unaffected by our adding more data


    sidenote: have a look at the numbers returned
    for :db/id and see that they're the same as the
    results for ?celestial-body
  "
  ([]
   (let [db0 (->
               (make-db)
               (d/db-with (get-schema))
               (d/db-with (Earth-based-observations)))
         db1 (d/db-with db0 (Jupiter-probe-observations))]
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

(defn brief-diversion-query-1a
  "
    Wait a minute, when we added the db schema
    we added it as data the same way we added data
    from our celestial observations -- couldn't
    we query the schema data too ?

    Well yes, we can -- let's find all entities
    representing attributes with type keyword

    (brief-diversion-query-1a)
    =>
    ([#:db{:id 3, :ident :has, :valueType :db.type/keyword}]
     [#:db{:id 2, :ident :orbits, :valueType :db.type/keyword}]
     [#:db{:id 1, :ident :name, :valueType :db.type/keyword}])

    that's our schema, bit predictable since all of them were
    type keyword
  "
  ([]
   (let [db (->
               (make-db)
               (d/db-with (get-schema))
               (d/db-with (Earth-based-observations)))]
     (d/q
       '{
         :find
         [
          [pull ?e [:db/id :db/ident :db/valueType]]
         ]
         :where
         [
           [?e :db/valueType :db.type/keyword]
         ]} db))))

(defn more-schema
  "
    Return the given database
    with a bit more schema transacted
  "
  ([]
   [{:db/ident :mass
     :db/valueType :db.type/double
     :db/cardinality :db.cardinality/one}]))

(defn measurements
  "
    Returns the given database
    with data from an experiment
  "
  []
  [
   {:name :Earth :mass 5.97237E1024}
   {:name :Venus :mass 4.8675E1024}
   ])

(defn brief-diversion-query-1b
  "
     Let's take the idea of
     schema-as-data further and see
     how we can add more schema
     and more elaborate data later

     (brief-diversion-query-1b)
     =>
      ([#:db{:id 3, :ident :has, :valueType :db.type/keyword}]
       [#:db{:id 8, :ident :mass, :valueType :db.type/double}]
       [#:db{:id 2, :ident :orbits, :valueType :db.type/keyword}]
       [#:db{:id 1, :ident :name, :valueType :db.type/keyword}])

     not only can we add new schema as our database matures,
     we can query the database to see our schema

     sidenote: see how in this query the :where clause
     doesn't have a value - it's just 2 elements not 3 -
     well you can do that, it means
     'where this entity has a value for this attribute'
     sidenote: in Datomic you can even query its representation
     of schemas
  "
  ([]
   (let [db (->
               (make-db)
               (d/db-with (get-schema))
               (d/db-with (Earth-based-observations))
               (d/db-with (more-schema))
               (d/db-with (measurements)))]
     (d/q
       '{
         :find
         [
          [pull ?e [:db/id :db/ident :db/valueType]]
         ]
         :where
         [
           [?e :db/valueType]
         ]} db))))

(defn more-schema-still
  "
    Return the given database
    with a bit more schema transacted
  "
  ([]
   [{:db/ident :mass
     :db/valueType :db.type/ref
     :db/cardinality :db.cardinality/one}
    {:db/ident :measurement/value
     :db/valueType :db.type/bigdec
     :db/cardinality :db.cardinality/one}
    {:db/ident :measurement/unit
     :db/valueType :db.type/keyword
     :db/cardinality :db.cardinality/one}]))

(defn proper-measurements
  "
    Returns the given database
    with data from an experiment
    but do it right this time
  "
  []
  [
   {:name :Earth :mass {:measurement/value 5.97237E1024M :measurement/unit :kg}}
   {:name :Venus :mass {:measurement/value 4.8675E1024M :measurement/unit :kg}}
   ])

(defn brief-diversion-query-1c
  "
     I changed my mind. Using doubles for
     mass measurements was a bad idea. I wish I'd
     used bigdecimals instead, and I wish I'd recorded
     the units too

     ...well, it's ok!

     We can change our mind because our
     database is a value made of facts, so
     let's add new facts about how we want
     to store data

     sidenote: we can always add new schema, but
     there are conditions for updating existing
     schema
  "
  ([]
   (let [db (->
               (make-db)
               (d/db-with (get-schema))
               (d/db-with (Earth-based-observations))
               (d/db-with (more-schema))
               (d/db-with (measurements))
               (d/db-with (more-schema-still))
               (d/db-with (proper-measurements)))]
     (d/q
       '{
         :find
         [
          [pull ?e [:name {:mass [:measurement/value :measurement/unit]}]]
          ]
         :where
         [
          [?e :mass ?m]
          [?m :measurement/value ?v]
          [(> ?v 10E23)]
          ]} db))))

(defn test-query-2
  "
    Getting back to the point
    about immutable database values,
    the thing about this is that we
    have the freedom to try speculative
    updates to our database to see what
    might happen, we can query the resulting
    database value and if we're happy we
    can actually transact it.

    This makes testing easy because we're
    free from all the potential bugs that
    come from managing state. We can create,
    update and query a database in a
    purely functional way with predictable
    results, simulating what it would be like
    to use the database in real life
  "
  ([]
   (let [db0 (-> (make-db)
                 (d/db-with (get-schema))
                 (d/db-with (more-schema-still)))
         dbs (reductions (fn [r f] (d/db-with r (f))) db0
               [Earth-based-observations Jupiter-probe-observations
                Saturn-probe-observations Venus-probe-observations])]
     (map
       (partial d/q
        '{
          :find
          [
           ?name
           [pull ?another-body [:name]]
           ]
          :where
          [
           [?celestial-body :orbits ?x]
           [?another-body :name ?x]
           [?celestial-body :name ?name]
           ]}) dbs))))

(defn test-query-3
  "
    Taking a closer look at the terms
    'entity' and 'attribute'

    Datoms representing facts

    [e a v]

    relate entities to attribute values.

    Attributes are stored as entities too,
    as we saw when we stored schema

    (test-query-3)
    => #{[6 :orbits 2] [5 :orbits 2] [8 :orbits 2]}

    in queries an attribute is specified by a keyword,
    the :db/indent of the entity representing that attribute.
    Hence, we see that :orbits is the :db/ident of entity 2,
    the entity representing the attribute :orbits

    sidenote: see how we can use functions in queries
    sidenote: see how clauses in :where don't always have to
    relate to eachother
  "
  ([]
    (->> (make-db)
      (d/db-with (get-schema))
      (d/db-with (Earth-based-observations))
      (d/q
        '{:find [?e ?a ?f]
          :where
            [
              [?e ?a :Sol]
              [(= ?a :orbits)]
              [?f :db/ident :orbits]]}))))

(defn test-query-4
  "
    Time

    We now know how to store facts
    and query the database but what
    about time ?

    How can we find out when a fact
    was stored ?

    Actually when facts are stored, the transaction
    and its time is stored too

    [entity attribute value tx]

    is a full datom and we can add time to our queries
    by using a variable in the 4th position of a clause

    Each transaction is an entity in itself and we can
    use the attribute :db/txInstant to get the time
    of the transaction in which a fact was stored


    sidenote: actually a full datom is

    [entity attribute value tx op]

    where op is a boolean indicating whether
    the fact stored was an assertion or retraction
  "
  ([]
    (->> (make-db)
      (d/db-with (get-schema))
      (d/db-with (Earth-based-observations))
      (d/q
        '{:find [?n ?t ?i]
          :where
          [
           [?e :orbits :Sol ?t]
           [?e :name ?n]
           [?t :db/txInstant ?i]
           ]}))))