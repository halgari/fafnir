# fafnir

A library that is designed to allow for easy insertion of complex data structures into Datomic.

We make use of the state monad to allow for large deeply nested structures to be inserted as a single Datomic transaction. 


## Usage

To use in your project, simply include it in your project.clj:

    [fafnir "1.0.2"]

Example Usage (from the tests):

    (testing "slightly more complex insert"
        (let [conn (make-db)]
          (is (= (do (-> (gen-plan
                          [id (assert-entity {:key/name "John" :key/age 42})
                           pid (assert-entity {:key/name "Bill" :key/age 72})
                           _ (update-entity id :key/parent pid)]
                          id)
                         (get-plan conn)
                         commit)
                     (-> (q '[:find ?child ?parent
                              :where
                              [?e :key/name ?child]
                              [?e :key/parent ?pid]
                              [?pid :key/name ?parent]]
                            (db conn))
                         first))
                 ["John" "Bill"]))))

## License

Copyright © 2013 Timothy Baldridge

Distributed under the Eclipse Public License, the same as Clojure.
