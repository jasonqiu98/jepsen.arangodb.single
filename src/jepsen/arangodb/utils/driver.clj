(ns jepsen.arangodb.utils.driver
  (:require [clojure.tools.logging :refer :all])
  (:import (com.arangodb DbName)
          ;;  com.arangodb.entity.BaseDocument
           com.arangodb.model.AqlQueryOptions))

(defn get-db
  "Get an arango db instance (type: ArangoDatabase) with its name"
  [conn db-name]
  ; db = conn.db(DbName.of(name))
  (-> conn (.db (DbName/of db-name))))

(defn create-db
  "Create a new database by name"
  [conn db-name]
  (-> conn (get-db db-name) (.create)))

(defn get-collection
  "Get an arangodb collection (type: ArangoCollection) by collection name"
  [conn db-name collection-name]
  ; collection = db.collection(collection-name)
  (-> conn (get-db db-name) (.collection collection-name)))

(defn create-collection
  "Create an arangodb collection by collection name"
  [conn db-name collection-name]
  (-> conn (get-collection db-name collection-name) (.create)))

;; (defn get-document
;;   "get arangodb document (type: BaseDocument) by key"
;;   [conn db-name collection-name doc-key]
;;   ; doc = collection.getDocument(key, BaseDocument.class)
;;   (-> conn (get-collection db-name collection-name) (.getDocument doc-key (.getClass (new BaseDocument)))))

; ----- OLD IMPLEMENTATION -----
;; (defn read-attr
;;   "Read one document attribute from an ArangoDB collection."
;;   [conn db-name collection-name doc-key attr-key]
;;   (try
;;     (-> conn (get-document db-name collection-name doc-key) (.getAttribute attr-key))
;;     (catch java.lang.NullPointerException e nil)))

; single operation, by Java Driver
(defn read-attr-type
  "Read one document attribute from an ArangoDB collection."
  [conn db-name collection-name doc-key attr-key data-type]
  ; FOR d IN example FILTER d._key == "1" RETURN d.val
  (let [query (str "FOR d IN " collection-name " FILTER d._key == " (str "\"" doc-key "\"") " RETURN d." attr-key)
        iter (-> conn (get-db db-name) (.query query data-type) (.iterator))]
    (if (.hasNext iter) (.next iter) nil)))

;; transactional ensured by AQL query
(defn write-attr
  "Update an attribute of a document if it exists,
   otherwise create a new attribute of that document;
   If the document does not exist,
   create the document and then create the attribute"
  [conn db-name collection-name doc-key attr-key attr-val]
  ; e.g. INSERT {_key: "1", val: 4} INTO example OPTIONS {overwriteMode: "update"} RETURN true
  (let [query (str "INSERT {_key: " (str "\"" doc-key "\"") ", " attr-key ": " attr-val "} INTO " collection-name
                   " OPTIONS {overwriteMode: \"update\"} RETURN true")]
    (-> conn (get-db db-name) (.query query Boolean) (.hasNext))))

;; transactional ensured by AQL query
(defn cas-attr
  "Set the document attribute to the new value if and only if
   the old value matches the current value of the attribute,
   and returns true. If the CaS fails, it returns false."
  [conn db-name collection-name doc-key attr-key old-val new-val]
  ; e.g. FOR d IN example FILTER d._key == "1" AND d.val == 4 UPDATE d WITH {val: 5} IN example RETURN true
  ; [true] for success / [] for failure
  (let [query (str "FOR d IN " collection-name " FILTER d._key == " (str "\"" doc-key "\"") " AND d."
                   attr-key " == " old-val " UPDATE d WITH {" attr-key ": " new-val "} IN " collection-name " RETURN true")]
    (-> conn (get-db db-name) (.query query Boolean) (.hasNext))))

;; (defn a-sample-txn
;;   [conn db-name collection-name attr-key doc-key attr-val]
;;   (let [db (-> conn (get-db db-name))
;;                    ; set write collections
;;         txn-entity (.beginStreamTransaction db (.writeCollections (new StreamTransactionOptions) (into-array [collection-name])))
;;         query-opts (-> (new AqlQueryOptions) (.streamTransactionId (.getId txn-entity)))
;;         a1-query (str "FOR d IN " collection-name " FILTER d._key == " (str "\"" doc-key "\"")
;;                       " UPDATE d WITH {" attr-key ": APPEND(d." attr-key ", " attr-val ")} IN "
;;                       collection-name " RETURN true")
;;         a2-query (str "FILTER LENGTH(FOR d IN " collection-name " FILTER d._key == " (str "\"" doc-key "\"")
;;                       " RETURN d." attr-key ") == 0 INSERT {_key: " (str "\"" doc-key "\"") ", " attr-key
;;                       ": [" attr-val "]} INTO " collection-name " RETURN true")
;;         res1  (if (-> db (.query a1-query query-opts Boolean) (.hasNext)) true ; if query returns, the list already exists
;;                   (-> db (.query a2-query query-opts Boolean) (.hasNext)))     ; otherwise, insert a new list
;;         r-query (str "FOR d IN " collection-name " FILTER d._key == " (str "\"" doc-key "\"") " RETURN d." attr-key)
;;         iter (-> db (.query r-query query-opts Object) (.iterator))
;;         res2 (if (.hasNext iter) (.next iter) nil)]
;;     (info (str [:r doc-key res2]))
;;     [:r doc-key res2]))

(defn submit-txn
  "Submit a transaction and get the return
   e.g. handle operation from {:type :invoke, :f :txn, :value [[:r 3 nil] [:append 3 2] [:r 3]]}
                 to {:type :ok, :f :txn, :value [[:r 3 [1]] [:append 3 2] [:r 3 [1 2]]]}"
  [conn db-name collection-name attr-key txn-vec txn-entity]
  (let [db (-> conn (get-db db-name))
        ret-val (map (fn [e] (let [[op doc-key attr-val] e]
                               (case op
                                 ; e.g. FOR d IN example FILTER d._key == "1" RETURN d.val
                                 ; remember to convert key from int to str
                                 :r (let [r-query (str "FOR d IN " collection-name " FILTER d._key == " (str "\"" doc-key "\"") " RETURN d." attr-key)
                                          query-opts (-> (new AqlQueryOptions) (.streamTransactionId (.getId txn-entity)))
                                          iter (-> db (.query r-query query-opts Object) (.iterator))
                                          res (vec (if (.hasNext iter) (.next iter) nil))]
                                      [op doc-key res])
                                 ; query 1
                                 ; FOR d IN example FILTER d._key == "1" UPDATE d WITH {val: APPEND(d.val, 4)} IN example RETURN true
                                 ; query 2
                                 ; FILTER LENGTH(FOR d IN example FILTER d._key == "1" RETURN d.val) == 0
                                 ; INSERT {_key: "1", val: [4]} INTO example RETURN true
                                 :append (let [a1-query (str "FOR d IN " collection-name " FILTER d._key == " (str "\"" doc-key "\"")
                                                             " UPDATE d WITH {" attr-key ": APPEND(d." attr-key ", " attr-val ")} IN "
                                                             collection-name " RETURN true")
                                               a2-query (str "FILTER LENGTH(FOR d IN " collection-name " FILTER d._key == " (str "\"" doc-key "\"")
                                                             " RETURN d." attr-key ") == 0 INSERT {_key: " (str "\"" doc-key "\"") ", " attr-key
                                                             ": [" attr-val "]} INTO " collection-name " RETURN true")
                                               query-opts (-> (new AqlQueryOptions) (.streamTransactionId (.getId txn-entity)))
                                               res  (if (-> db (.query a1-query query-opts Boolean) (.hasNext)) true ; if query returns, the list already exists
                                                        (-> db (.query a2-query query-opts Boolean) (.hasNext)))     ; otherwise, insert a new list
                                               ]
                                           (when (not res) (throw (Exception. "append fails!")))
                                           e))))
                     txn-vec)]
    (if (= ret-val nil) nil (vec ret-val))))

