(ns fafnir.core
  (:require [datomic.api :refer [db q] :as d]))

;; Fafnir is a library that attempts to ease development of complex
;; transactions with datomic. We use a form of the state monad to
;; build up transaction "plans". These plans can be composed, and
;; commited to a Datomic connection. This entire library is immutable
;; and purely functional. No reference variables are used, and the
;; Datomic DB is only defreferenced once per commit.

(defn commit
  "Commit processes the transaction with the associated connection, then updates all the tempids to match. You can then use plan-id to get the realized ent-ids"
  [{:keys [conn db new-ents updates valid-ids] :as plan}]
  (assert (and conn db))
  (let [ents (reduce
              (fn [acc [ent id]]
                (assert (not (get acc id)) "Duplicate ids")
                (assoc acc id (assoc ent :db/id id)))
              {}
              new-ents)
        _ (assert (= (set (keys ents))
                     (set (keys valid-ids)))
                  (pr-str (count (set (keys ents)))
                          (count (set (keys valid-ids)))
                          (count new-ents)))

        data (concat #_(mapcat (fn [ent]
                               (let [ent (dissoc ent :db/id)]
                                 (map (partial vector :db/add (:db/id ent))
                                      (keys ent)
                                      (vals ent))))
                               (vals ents))
                     (vals ents)
                     (map (fn [[id k v]]
                            [:db/add id k v])
                          updates))
        {:keys [db-before db-after tempids tx-data]}
        @(d/transact conn data)
        ptempids (zipmap
                  (keys (:tempids plan))
                  (map (partial d/resolve-tempid db-after tempids)
                       (vals (:tempids plan))))]
    (assoc plan
      :tempids ptempids
      :db db-after
      :db-before db-before
      :new-ents nil
      :singletons nil)))

(defn plan-id 
  [plan val]
  (if-let [v (get-in plan [:tempids val])]
    v
    (assert false (str "Can't find " val))))

(defn plan-ent
  [plan val]
  (d/entity (:db plan) (get-in plan [:tempids val])))

(defn to-seq [head]
  (when head
    (cons (:list/head head)
          (lazy-seq (to-seq (:list/tail head))))))

(defn- get-query
  "Used by find-singleton to construct an on-the-fly query for finding a singleton"
  [sing]
  `[:find ~'?id
    :where
    ~@(map (fn [[k v]]
             (vector '?id k v))
           sing)])

(defn- find-singleton
  "Queries the DB for a singleton who's attibutes and values match the input. Returns nil if none are found."
  [db sing]
  (assert db (pr-str db))
  (let [results (->> (q (get-query sing) db)
                     (filter (fn [[id]]
                               (= (count sing)
                                  (count (d/touch (d/entity db id)))))))]
    (assert (>= 1 (count results)))
    (ffirst results)))

(defn singleton
  "Given an entity, returns either a tempid if the entity does not exist in the DB or returns the existing id. For a entity to be considered a \"singleton\" very attribute and value must exist in the DB version of the entity. That is to say, it must be a perfect match."
  ([sing]
     (singleton sing nil))
  ([sing key]
      (fn [plan]
        (if-let [id (get-in plan [:singletons sing])]
          [id plan]
          (if-let [q (find-singleton (:db plan) sing)]
            [q (assoc-in plan [:singletons sing] q)]
            (let [newid (d/tempid :db.part/user)]
              [newid (-> plan
                         (assoc-in [:singletons sing] newid)
                         (assoc-in [:new-ents sing] newid)
                         (assoc-in [:tempids key] newid)
                         (assoc-in [:valid-ids newid] newid))]))))))

(defn assert-entity
  "Adds an entity to the DB, if no :db/id is provided one will be generated. Returns the :db/id of the new entity. "
  ([ent]
     (assert-entity ent nil))
  ([ent key]
      (fn [plan]
        (let [newid (if-let [id (:db/id ent)]
                      id
                      (d/tempid :db.part/user))
              ent (assoc ent :db/id newid)]
          (assert (not= 1 (count ent)) (str "Cannot assert a empty entity " ent))
          [newid (-> plan
                     (assoc-in [:new-ents ent] newid)
                     (assoc-in [:tempids key] newid)
                     (assoc-in [:valid-ids newid] newid))]))))

(defn update-entity
  "Updates an existing entity. This is the same as transacting [:db/add ent attr val]."
  [ent & attrs-vals]
  (let [pairs (partition 2 attrs-vals)]
    (fn [plan]
      (assert (get (:valid-ids plan) ent) (pr-str "Must give entity id" ent "=>" (:valid-ids plan)))
      (let [new-plan (reduce
                      (fn [plan [k v]]
                        (update-in plan [:updates] (fnil conj []) [ent k v]))
                      plan
                      pairs)]
        [ent new-plan]))))

(defn update-all
  "Takes a set of datoms and adds them to the list of datoms to be asserted in the transaction"
  [itms]
  (fn [plan]
    (let [new-plan (reduce
                    (fn [plan data]
                      (update-in plan [:updates] (fnil conj []) data))
                    plan
                    itms)]
      [nil new-plan])))

(defn all
  "Given a list of plans, add them all to the current plan.

   Example:

    (gen-plan
      [ids (all (map assert-entity ents))]
      ids)"
  [itms]
  (fn [plan]
    (reduce
     (fn [[ids plan] f]
       (let [[id plan] (f plan)]
         [(conj ids id) plan]))
     [[] plan]
     itms)))

(defmacro gen-plan
  "Takes a set of operations to perform to the DB and threads the state map through them all. This allows a transaction plan to be built without worrying about explicitly tracking the state map.
  Example:
   (gen-plan
     [pid (assert-entity {:attr/code 42})
      cid (assert-entity {:attr/code 45 :parent/id pid})])"
  [binds id-expr]
  (let [binds (partition 2 binds)
        psym (gensym "plan_")
        forms (reduce
               (fn [acc [id expr]]
                 (concat acc `[[~id ~psym] (~expr ~psym)]))
               []
               binds)]
    `(fn [~psym]
       (let [~@forms]
         [~id-expr ~psym]))))

(defn assoc-plan
  "Like assoc but uses the plan as the map"
  [key val]
  (fn [plan]
    [nil (assoc-in plan [:user key] val)]))

(defn assoc-in-plan
  "Like clojure.core/assoc-in but allows the user to assoc data into the plan"
  [path val]
  (fn [plan]
    [nil (assoc-in plan (vec (list* :user path)) val)]))

(defn update-in-plan
  "Like clojure.core/update-in but allows the user to modify the state of the plan"
  [path f & args]
  (fn [plan]
    [nil (apply update-in plan (vec (list* :user path)) f args)]))

(defn get-in-plan [path]
  (fn [plan]
    [(get-in plan (list* :user path)) plan]))

(defn push-binding [key value]
  (fn [plan]
    [nil (update-in plan [:bindings key] conj value)]))

(defn push-alter-binding [key f & args]
  (fn [plan]
    [nil (update-in plan [:bindings key]
                  #(conj % (apply f (first %) args)))]))

(defn get-binding [key]
  (fn [plan]
    [(first (get-in plan [:bindings key])) plan]))

(defn pop-binding [key]
  (fn [plan]
    [(first (get-in plan [:bindings key]))
     (update-in plan [:bindgins key] pop)]))

(def no-op
  "Used to terminate the else leg of a if clause in a gen-plan.

  Example:

  (gen-plan
   [id (if pred?
          (assert-entity ent)
          no-op)])"
  (fn [plan]
    [nil plan]))

(defn get-plan [planval conn]
  (assert (ifn? planval))
  (let [val (planval {:conn conn
                      :db (db conn)
                      :singletons {}
                      :new-ents {}
                      :tempids {}})]
    (assert (vector? val))
    (second val)))

(defn- assert-list-node [last id]
  (gen-plan
   [ent-id (let [ent (merge
                      (if last
                        {:list/tail last}
                        {})
                      {:list/head id})]
             (singleton ent ent))]
   ent-id))

(defn assert-seq [seq]
  (fn [plan]
    (reduce
     (fn [[last-id plan] id]
       ((assert-list-node last-id id) plan))
     [nil plan]
     (reverse seq))))

