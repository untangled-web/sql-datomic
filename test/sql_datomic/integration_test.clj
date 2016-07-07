(ns sql-datomic.integration-test
  (:require [clojure.test :refer :all]
            [sql-datomic.datomic :as dat]
            sql-datomic.types  ;; necessary for reader literals
            [sql-datomic.parser :as par]
            [sql-datomic.select-command :as sel]
            [sql-datomic.insert-command :as ins]
            [sql-datomic.update-command :as upd]
            [sql-datomic.delete-command :as del]
            [sql-datomic.retract-command :as rtr]
            [datomic.api :as d]
            [clojure.string :as str]
            [clojure.set :as set]))


;;;; SETUP and HELPER FUNCTIONS ;;;;

(def ^:dynamic *conn* :not-a-connection)

(def ^:dynamic *db* :not-a-db)

(defn current-db [] (d/db *conn*))

(defn entity->map
  ([eid] (entity->map *db* eid))
  ([db eid]
   (if-let [e (d/entity db eid)]
     (->> e d/touch (into {:db/id (:db/id e)}))
     {})))

(defn product-map->comparable [m]
  ;; cannot compare byte arrays and :db/id changes per db refresh
  (dissoc m :db/id :product/blob))

(defn -select-keys [ks m]
  (select-keys m ks))

(defn -select-resultset [{:keys [entities attrs]}]
  (into #{} (map (partial -select-keys attrs) entities)))

(defn count-entities
  ([db primary-attr]
   (let [n (d/q '[:find (count ?e) .
                  :in $ ?attr
                  :where [?e ?attr]]
                db
                primary-attr)]
     (if (nil? n) 0 n)))
  ([db attr v]
   (let [n (d/q '[:find (count ?e) .
                  :in $ ?attr ?v
                  :where [?e ?attr ?v]]
                db
                attr v)]
     (if (nil? n) 0 n))))

(defn db-fixture [f]
  (let [sys (.start (dat/system {}))]
    #_(do (println "=== db set-up") (flush))
    (binding [*conn* (->> sys :datomic :connection)]
      (binding [*db* (d/db *conn*)]
        (f)))
    (.stop sys)
    #_(do (println "=== db tear-down") (flush))))

(use-fixtures :each db-fixture)


;;;; SELECT statements ;;;;

(deftest product-entity-present
  (is (= (product-map->comparable (entity->map *db* [:product/prod-id 8293]))
         {:product/url #uri "http://example.com/products/8293"
          :product/prod-id 8293
          :product/uuid #uuid "57607472-0bd8-4ed3-98d3-586e9e9c9683"
          :product/common-prod-id 4804
          :product/man-hours 100N
          :product/price 13.99M
          :product/category :product.category/family
          :product/tag :alabama-exorcist-family
          :product/actor "NICK MINELLI"
          :product/rating #float 2.6
          :product/special false
          :product/title "ALABAMA EXORCIST"})))

(defn stmt->ir [stmt]
  (->> stmt par/parser par/transform))

(deftest select-product-by-prod-id
  (let [ir (stmt->ir "select where product.prod-id = 9990")]
    (is (= (->> (sel/run-select *db* ir)
                :entities
                (map product-map->comparable))
           [{:product/url #uri "http://example.com/products/9990"
             :product/prod-id 9990
             :product/uuid #uuid "57607426-cdd4-49fa-aecb-0a2572976db9"
             :product/common-prod-id 6584
             :product/man-hours 60100200300N
             :product/price 25.99M
             :product/category :product.category/music
             :product/tag :aladdin-world-music
             :product/actor "HUMPHREY DENCH"
             :product/rating #float 2.0
             :product/special false
             :product/title "ALADDIN WORLD"}]))))

(deftest select-all-products-by-prod-id
  (let [ir (stmt->ir "select where product.prod-id > 0")
        prod-ids #{1298 1567
                   2290 2926
                   4402 4936
                   5130
                   6127 6376 6879
                   8293
                   9990}]
    (is (= (->> (sel/run-select *db* ir)
                :entities
                (map :product/prod-id)
                (into #{}))
           prod-ids))))

(deftest select-prod-id-6k-products
  (let [ir (stmt->ir
            "select where product.prod-id between 6000 and 6999")
        prod-ids #{6127 6376 6879}]
    (is (= (->> (sel/run-select *db* ir)
                :entities
                (map :product/prod-id)
                (into #{}))
           prod-ids))))

(deftest select-no-products-by-prod-id
  (let [ir (stmt->ir
            "select where product.prod-id >= 10000")
        prod-ids #{}]
    (is (= (->> (sel/run-select *db* ir)
                :entities
                (map :product/prod-id)
                (into #{}))
           prod-ids))))

(deftest select-order-between-dates
  (let [ir (stmt->ir
            "select where
             order.orderdate between #inst \"2004-01-01\"
                                 and #inst \"2004-01-05\" ")]
    (is (= (->> (sel/run-select *db* ir)
                :entities
                (map (partial -select-keys
                              [:order/orderid :order/orderdate]))
                (into #{}))
            #{{:order/orderid 2
               :order/orderdate #inst "2004-01-01T08:00:00.000-00:00"}}))))

(deftest select-cols-of-product-by-prod-id
  (let [ir (stmt->ir
            "select product.prod-id, #attr :product/tag, product.title
             where product.prod-id = 9990")]
    (is (= (-select-resultset (sel/run-select *db* ir))
           #{{:product/prod-id 9990
              :product/tag :aladdin-world-music
              :product/title "ALADDIN WORLD"}}))))

(deftest select-join
  (let [ir (stmt->ir
            "select order.orderid,
                    order.totalamount,
                    order.customerid,
                    customer.email,
                    orderline.orderlineid,
                    orderline.prod-id,
                    product.uuid,
                    product.category,
                    orderline.quantity
              where order.orderid     = orderline.orderid
                and order.customerid  = customer.customerid
                and orderline.prod-id = product.prod-id

                and order.orderid in (1, 2, 3)
                and orderline.quantity > 2
                and product.category <> :product.category/new
            ")]
    (is (= (-select-resultset (sel/run-select *db* ir))
           #{{:order/orderid 2
              :order/totalamount 59.43M
              :order/customerid 4858
              :customer/email "OVWOIYIDDL@dell.com"
              :orderline/orderlineid 8
              :orderline/prod-id 2926
              :product/uuid #uuid "576073e7-d671-45ba-af1a-f08a9a355b81"
              :product/category :product.category/music
              :orderline/quantity 3}
             {:order/orderid 2
              :order/totalamount 59.43M
              :order/customerid 4858
              :customer/email "OVWOIYIDDL@dell.com"
              :orderline/orderlineid 4
              :orderline/prod-id 5130
              :product/uuid #uuid "5760740c-4f24-4f3d-8455-f79a1cc57fa9"
              :product/category :product.category/action
              :orderline/quantity 3}}))))

;; | :product/prod-id | :product/rating | :product/price |             :product/category | :product/man-hours | cat | pr | mh | ??
;; |------------------+-----------------+----------------+-------------------------------+--------------------|            
;; |             2926 |      #float 1.6 |         22.99M |       :product.category/music |       40100200300N | t     f    f    t
;; |             5130 |      #float 1.8 |         14.99M |      :product.category/action |       50100200300N | t     t    f    t
;; |             9990 |      #float 2.0 |         25.99M |       :product.category/music |       60100200300N | t     f    f    t
;; |             1567 |      #float 2.2 |         25.99M |         :product.category/new |       70100200300N | f     f    f    f
;; |             6376 |      #float 2.4 |         21.99M |       :product.category/drama |       80100200300N | f     f    f    f
;; |             8293 |      #float 2.6 |         13.99M |      :product.category/family |               100N | t     t    t    t
;; |             4402 |      #float 2.8 |         11.99M |    :product.category/children |               101N | f     t    t    t
;; |             6879 |      #float 3.0 |         12.99M |      :product.category/action |               102N | t     t    t    t
;; |             2290 |      #float 3.2 |         15.99M | :product.category/documentary |               103N | f     t    t    t
(deftest select-and-or-not-oh-my
  (let [ir (stmt->ir
            "select product.prod-id,
                    product.category,
                    product.price,
                    product.man-hours,
                    product.rating
              where (   product.category in (
                          :product.category/music,
                          :product.category/action,
                          :product.category/family,
                          :product.category/horror)
                     or (    (not (product.price between 20.0M and 30.0M))
                         and (not (product.man-hours > 1000N))))
                and product.rating > 1.5f")]
    (is (= (-select-resultset (sel/run-select *db* ir))
           #{{:product/prod-id 2926
              :product/rating #float 1.6
              :product/price 22.99M
              :product/category :product.category/music
              :product/man-hours 40100200300N}
             {:product/prod-id 5130
              :product/rating #float 1.8
              :product/price 14.99M
              :product/category :product.category/action
              :product/man-hours 50100200300N}
             {:product/prod-id 9990
              :product/rating #float 2.0
              :product/price 25.99M
              :product/category :product.category/music
              :product/man-hours 60100200300N}
             {:product/prod-id 8293
              :product/rating #float 2.6
              :product/price 13.99M
              :product/category :product.category/family
              :product/man-hours 100N}
             {:product/prod-id 4402
              :product/rating #float 2.8
              :product/price 11.99M
              :product/category :product.category/children
              :product/man-hours 101N}
             {:product/prod-id 6879
              :product/rating #float 3.0
              :product/price 12.99M
              :product/category :product.category/action
              :product/man-hours 102N}
             {:product/prod-id 2290
              :product/rating #float 3.2
              :product/price 15.99M
              :product/category :product.category/documentary
              :product/man-hours 103N}}))))

(deftest select-by-db-id
  (let [db-ids (d/q '[:find [?e ...]
                      :where
                      [?e :product/prod-id ?pid]
                      [(> ?pid 6500)]]
                    *db*)
        stmt (str "select product.prod-id where #attr :db/id "
                  (str/join " " db-ids))
        ir (stmt->ir stmt)]
    (is (= (-select-resultset (sel/run-select *db* ir))
           #{{:product/prod-id 9990}
             {:product/prod-id 8293}
             {:product/prod-id 6879}}))))


;;;; INSERT statements ;;;;

(deftest insert-traditional-form
  (let [db *db*
        cnt (count-entities db :customer/customerid)
        stmt "insert into customer (
                 customerid,
                 firstname, lastname,    email,  address-1,
                      city,    state,      zip,    country
              )
              values (
                  12345,
                  'Foo', 'Bar', 'foobar@example.org', '123 Some Place',
                  'Thousand Oaks', 'CA', '91362', 'USA'
              )"
        ir (stmt->ir stmt)
        _got (ins/run-insert *conn* ir {:silent true})
        db' (d/db *conn*)
        e->m' (partial entity->map db')
        cnt' (count-entities db' :customer/customerid)
        ids (d/q '[:find [?e ...]
                   :where [?e :customer/email "foobar@example.org"]]
                 db')
        ents (->> ids
                  (map e->m')
                  (map (fn [m] (dissoc m :db/id)))
                  (into #{}))]
    (is (= (inc cnt) cnt'))
    (is (= ents
           #{{:customer/customerid 12345
              :customer/firstname "Foo"
              :customer/lastname "Bar"
              :customer/email "foobar@example.org"
              :customer/address-1 "123 Some Place"
              :customer/city "Thousand Oaks"
              :customer/state "CA"
              :customer/zip "91362"
              :customer/country "USA"}}))))

(deftest insert-short-form
  (let [db *db*
        cnt (count-entities db :product/prod-id)
        stmt "insert into
              #attr :product/prod-id = 9999,
              #attr :product/actor = 'Naomi Watts',
              #attr :product/title = 'The Ring',
                     product.category = :product.category/horror,
                     product.rating = 4.5f,
                     product.man-hours = 9001N,
                     product.price = 21.99M"
        ir (stmt->ir stmt)
        _got (ins/run-insert *conn* ir {:silent true})
        db' (d/db *conn*)
        e->m' (partial entity->map db')
        cnt' (count-entities db' :product/prod-id)
        ids (d/q '[:find [?e ...]
                   :where [?e :product/prod-id 9999]]
                 db')
        ents (->> ids
                  (map e->m')
                  (map (fn [m] (dissoc m :db/id)))
                  (into #{}))]
    (is (= (inc cnt) cnt'))
    (is (= ents
           #{{:product/prod-id 9999
              :product/actor "Naomi Watts"
              :product/title "The Ring"
              :product/category :product.category/horror
              :product/rating #float 4.5
              :product/man-hours 9001N
              :product/price 21.99M}}))))


;;;; UPDATE statements ;;;;

(deftest update-customer-where-customerid
  (let [stmt "update      customer
              set         customer.city = 'Springfield'
                        , customer.state = 'VA'
                        , customer.zip = '22150'
              where       customer.customerid = 4858"
        db *db*
        id (d/q '[:find ?e . :where [?e :customer/customerid 4858]] db)
        ent (entity->map db id)
        cnt (count-entities db :customer/customerid)
        ir (stmt->ir stmt)
        _got (upd/run-update *conn* db ir {:silent true})
        db' (d/db *conn*)
        ent' (entity->map db' id)]
    (is (= ent'
           (assoc ent
                  :customer/city "Springfield"
                  :customer/state "VA"
                  :customer/zip "22150")))
    (is (= (count-entities db' :customer/customerid) cnt)
        "number of customers remains unchanged")))

(deftest update-products-where-db-id
  (let [db *db*
        ids (d/q '[:find [?e ...]
                   :where
                   [?e :product/prod-id ?pid]
                   [(> ?pid 6000)]]
                 db)
        e->m (partial entity->map db)
        ents (->> ids (map e->m) (into #{}))
        cnt (count-entities db :product/prod-id)
        stmt "update      product
              set         product.tag = :all-the-things
                        , product.special = true
                        , product.price = 1.50M
              where       db.id "
        stmt' (str stmt (str/join " " ids))
        ir (stmt->ir stmt')
        _got (upd/run-update *conn* db ir {:silent true})
        db' (d/db *conn*)
        e->m' (partial entity->map db')
        ents' (->> ids (map e->m') (into #{}))]
    (is (= ents'
           (->> ents
                (map (fn [ent]
                       (assoc ent
                              :product/tag :all-the-things
                              :product/special true
                              :product/price 1.50M)))
                (into #{}))))
    (is (= (count-entities db' :product/prod-id) cnt)
        "the number of products has not changed")
    ;; assumption: all products originally have special set to false
    (when (zero? (count-entities db :product/special true))
      (is (= (count ents')
             (count-entities db' :product/special true))))))

(deftest update-customer-order-join
  (let [stmt "update customer
              set    #attr :customer/age = 45
                   , customer.income = 70000M
              where  customer.customerid = order.customerid
                 and order.orderid = 5"
        db *db*
        e->m (partial entity->map db)
        the-order (e->m [:order/orderid 5])
        the-cust  (:order/customer the-order)
        order-cnt (count-entities db :order/orderid)
        cust-cnt (count-entities db :customer/customerid)
        ir (stmt->ir stmt)
        _got (upd/run-update *conn* db ir {:silent true})
        db' (d/db *conn*)
        e->m' (partial entity->map db')]
    (is (= (e->m' [:order/orderid 5])
           the-order)
        "the order was unaffected")
    (is (= (e->m' (:db/id the-cust))
           (assoc (e->m (:db/id the-cust))
                  :customer/age 45
                  :customer/income 70000M)))
    (is (= (count-entities db' :order/orderid) order-cnt)
        "the number of orders has not changed")
    (is (= (count-entities db' :customer/customerid) cust-cnt)
        "the number of customers has not changed")))

(deftest update-product-short-form
  (let [stmt "update #attr :product/rating = 3.5f
               where #attr :product/prod-id > 6000"
        db *db*
        e->m (partial entity->map db)
        ids (d/q '[:find [?e ...]
                   :where
                   [?e :product/prod-id ?pid]
                   [(> ?pid 6000)]]
                 db)
        products (->> ids (map e->m) (into #{}))
        cnt (count-entities db :product/prod-id)
        ir (stmt->ir stmt)
        _got (upd/run-update *conn* db ir {:silent true})
        db' (d/db *conn*)
        e->m' (partial entity->map db')
        products' (->> ids (map e->m') (into #{}))]
    (is (= products'
           (->> products
                (map (fn [p] (assoc p :product/rating #float 3.5)))
                (into #{}))))
    (is (= (count-entities db' :product/prod-id) cnt)
        "the number of products has not changed")))

(deftest update-product-short-form-by-db-id
  (let [stmt "update #attr :product/rating = 3.5f
               where #attr :db/id in ("
        db *db*
        e->m (partial entity->map db)
        ids (d/q '[:find [?e ...]
                   :where
                   [?e :product/prod-id ?pid]
                   [(< ?pid 6000)]]
                 db)
        stmt' (str stmt (str/join ", " ids) ")")
        products (->> ids (map e->m) (into #{}))
        cnt (count-entities db :product/prod-id)
        ir (stmt->ir stmt')
        _got (upd/run-update *conn* db ir {:silent true})
        db' (d/db *conn*)
        e->m' (partial entity->map db')
        products' (->> ids (map e->m') (into #{}))]
    (is (= products'
           (->> products
                (map (fn [p] (assoc p :product/rating #float 3.5)))
                (into #{}))))
    (is (= (count-entities db' :product/prod-id) cnt)
        "the number of products has not changed")))

(deftest update-customer-order-join-short-form
  (let [stmt "update #attr :customer/age = 45
                   , customer.income = 70000M
               where customer.customerid = order.customerid
                 and order.orderid = 5"
        ;; this works so long as all attrs in the assignment pair list
        ;; refer to one (and only one) namespace/table.
        db *db*
        e->m (partial entity->map db)
        the-order (e->m [:order/orderid 5])
        the-cust  (:order/customer the-order)
        order-cnt (count-entities db :order/orderid)
        cust-cnt (count-entities db :customer/customerid)
        ir (stmt->ir stmt)
        _got (upd/run-update *conn* db ir {:silent true})
        db' (d/db *conn*)
        e->m' (partial entity->map db')]
    (is (= (e->m' [:order/orderid 5])
           the-order)
        "the order was unaffected")
    (is (= (e->m' (:db/id the-cust))
           (assoc (e->m (:db/id the-cust))
                  :customer/age 45
                  :customer/income 70000M)))
    (is (= (count-entities db' :order/orderid) order-cnt)
        "the number of orders has not changed")
    (is (= (count-entities db' :customer/customerid) cust-cnt)
        "the number of customers has not changed")))


;;;; DELETE statements ;;;;

(deftest delete-targeting-no-rows-has-no-effect
  (let [actor "homer simpson"
        stmt (format "delete from product where product.actor = '%s'"
                     actor)
        db *db*
        product-cnt (count-entities db :product/prod-id)
        ;; should be empty
        ids (d/q '[:find [?e ...]
                   :in $ ?doh!
                   :where [?e :product/actor ?doh!]]
                 db actor)
        ir (stmt->ir stmt)
        _got (del/run-delete *conn* db ir {:silent true})
        db' (d/db *conn*)]
    (is (empty? ids) "assumption: where clause matched no rows")
    (is (= (count-entities db' :product/prod-id)
           product-cnt)
        "the number of products has not changed")
    (is (= db db') "the db itself has not changed")))

(deftest delete-product-by-prod-id
  (let [prod-id 9990
        stmt (format "delete from product where product.prod-id = %d"
                     prod-id)
        db *db*
        e->m (partial entity->map db)
        query '[:find ?e
                :in $ ?pid
                :where [?e :product/prod-id ?pid]]
        product (e->m [:product/prod-id prod-id])
        product-cnt (count-entities db :product/prod-id)
        ir (stmt->ir stmt)
        _got (del/run-delete *conn* db ir {:silent true})
        db' (d/db *conn*)
        e->m' (partial entity->map db')]
    (is (not-empty (d/q query db prod-id))
        "assumption: product targeted for deletion was present initially")
    (is (= (count-entities db' :product/prod-id)
           (dec product-cnt))
        "the number of products has decreased by one")
    (is (empty? (d/q query db' prod-id))
        "product targeted for deletion was retracted")))

(deftest delete-all-products-by-prod-id
  ;; chose products here because they have no component attrs,
  ;; i.e., they will not trigger a cascading delete.
  (let [stmt "delete from product where product.prod-id > 0"
        db *db*
        query '[:find [?e ...]
                :where [?e :product/prod-id]]
        ids (d/q query db)
        ir (stmt->ir stmt)
        _got (del/run-delete *conn* db ir {:silent true})
        db' (d/db *conn*)]
    (is (pos? (count-entities db :product/prod-id))
        "assumption: initially, db had products present")
    (is (zero? (count-entities db' :product/prod-id))
        "the number of products has decreased to zero")
    (is (empty? (d/q query db'))
        "no products are present in db")))

(deftest delete-products-by-db-id
  (let [stmt "delete from product where db.id "
        db *db*
        e->m (partial entity->map db)
        product-cnt (count-entities db :product/prod-id)
        query '[:find [?e ...]
                :where
                [?e :product/prod-id ?pid]
                [(<= ?pid 7000)]
                [(<= 4000 ?pid)]]
        ids (d/q query db)
        stmt' (str stmt (str/join " " ids))
        ir (stmt->ir stmt')
        _got (del/run-delete *conn* db ir {:silent true})
        db' (d/db *conn*)
        e->m' (partial entity->map db')
        query' '[:find [?e ...] :where [?e :product/prod-id]]
        ids' (d/q query' db')
        products' (map e->m' ids')]
    (is (= (count-entities db' :product/prod-id)
           (- product-cnt (count ids)))
        "the number of products has decreased appropriately")
    (is (empty? (set/intersection
                 (into #{} ids')
                 (into #{} ids)))
        "targeted :db/ids no longer present in db")
    (is (not-any? (fn [{:keys [product/prod-id]}]
                    (<= 4000 prod-id 7000))
                  products'))))

(deftest delete-order-cascading-orderlines-by-orderid
  (let [stmt "delete from order where order.orderid = 5"
        db *db*
        e->m (partial entity->map db)
        ;; an order has many orderlines (as components)
        ;; an orderline has a product (not component)
        ;; deleting an order should delete its orderlines but not the products
        order-cnt (count-entities db :order/orderid)
        orderline-cnt (count-entities db :orderline/orderlineid)
        product-cnt (count-entities db :product/prod-id)
        oid (d/q '[:find ?e . :where [?e :order/orderid 5]] db)
        order (e->m oid)
        olids (d/q '[:find [?e ...] :where [?e :orderline/orderid 5]] db)
        orderlines (map e->m olids)
        pids (d/q '[:find [?e ...]
                    :where
                    [?ol :orderline/orderid 5]
                    [?ol :orderline/prod-id ?pid]
                    [?e :product/prod-id ?pid]] db)
        products (map e->m pids)
        prod-ids (map :product/prod-id products)
        ir (stmt->ir stmt)
        _got (del/run-delete *conn* db ir {:silent true})
        db' (d/db *conn*)
        e->m' (partial entity->map db')]
    (is (empty? (d/q '[:find ?e :where [?e :order/orderid 5]] db'))
        "targeted order is no longer present in db")
    (is (empty? (d/q '[:find ?e :where [?e :orderline/orderid 5]] db'))
        "targeted order's orderlines are no longer present in db")
    (is (not-empty
         (d/q '[:find ?e
                :in $ [?pid ...]
                :where [?e :product/prod-id ?pid]]
              db' prod-ids))
        "targeted order's orderlines' products are still present in db")
    (is (= (count-entities db' :order/orderid)
           (dec order-cnt))
        "the number of orders has decreased by one")
    (is (= (count-entities db' :orderline/orderlineid)
           (- orderline-cnt (count olids)))
        "the number of orderlines has decreased appropriately")
    (is (= (count-entities db' :product/prod-id)
           product-cnt)
        "the number of products has not changed")))

(deftest delete-product-by-prod-id-short-form
  (let [prod-id 1567
        stmt (format "delete where #attr :product/prod-id = %d"
                     prod-id)
        db *db*
        e->m (partial entity->map db)
        query '[:find ?e
                :in $ ?pid
                :where [?e :product/prod-id ?pid]]
        product (e->m [:product/prod-id prod-id])
        product-cnt (count-entities db :product/prod-id)
        ir (stmt->ir stmt)
        _got (del/run-delete *conn* db ir {:silent true})
        db' (d/db *conn*)
        e->m' (partial entity->map db')]
    (is (not-empty (d/q query db prod-id))
        "assumption: product targeted for deletion was present initially")
    (is (= (count-entities db' :product/prod-id)
           (dec product-cnt))
        "the number of products has decreased by one")
    (is (empty? (d/q query db' prod-id))
        "product targeted for deletion was retracted")))

(deftest delete-orderlines-join-by-orderid
  (let [stmt "delete from orderline
               where orderline.orderid = order.orderid
                 and order.orderid = 2
                 and orderline.quantity = 1"
        ;; note that we are targeting orderlines for deletion, not order
        db *db*
        e->m (partial entity->map db)
        order-cnt (count-entities db :order/orderid)
        orderline-cnt (count-entities db :orderline/orderlineid)
        product-cnt (count-entities db :product/prod-id)
        target-olids (d/q '[:find [?e ...]
                            :where
                            [?e :orderline/orderid 2]
                            [?e :orderline/quantity 1]] db)
        target-orderline-ids (->> target-olids
                                  (map e->m)
                                  (map :orderline/orderlineid))
        remain-olids (d/q '[:find [?e ...]
                            :where
                            [?e :orderline/orderid 2]
                            [?e :orderline/quantity ?n]
                            [(not= ?n 1)]] db)
        pids (d/q '[:find [?e ...]
                    :where
                    [?ol :orderline/orderid 2]
                    [?ol :orderline/prod-id ?pid]
                    [?e :product/prod-id ?pid]] db)
        prod-ids (->> pids
                      (map e->m)
                      (map :product/prod-id))
        ir (stmt->ir stmt)
        _got (del/run-delete *conn* db ir {:silent true})
        db' (d/db *conn*)
        e->m' (partial entity->map db')
        orderlines' (->> (d/q '[:find [?e ...]
                                :where [?e :orderline/orderid 2]] db')
                         (map e->m'))]
    (is (= (count-entities db' :order/orderid)
           order-cnt)
        "the number of orders has not changed")
    (is (= (count-entities db' :orderline/orderlineid)
           (- orderline-cnt (count target-olids)))
        "the number of orderlines has decreased appropriately")
    (is (= (count-entities db' :product/prod-id)
           product-cnt)
        "the number of products has not changed")
    (is (not-empty (d/q '[:find ?e :where [?e :order/orderid 2]] db'))
        "the associated order is still present")
    (is (empty?
         (d/q '[:find ?e
                :in $ [?olid ...]
                :where
                [?e :orderline/orderid 2]
                [?e :orderline/orderlineid ?olid]]
              db' target-orderline-ids))
        "targeted orderlines of given order were retracted")
    (is (not-any? (fn [{:keys [orderline/quantity]}]
                    (= 1 quantity))
                  orderlines')
        "none of the quantity orderlines of given order are present")
    (is (= (->> (d/q '[:find [?e ...] :where [?e :orderline/orderid 2]] db')
                (into #{}))
           (->> remain-olids (into #{})))
        "all of the non-targeted orderlines are still present")))


;;;; RETRACT statements ;;;;

(deftest retract-product-attr-by-db-id
  (let [stmt-fmt "RETRACT product.uuid WHERE db.id = %d"
        db *db*
        e->m (partial entity->map db)
        product-cnt (count-entities db :product/prod-id)
        prod-id 8293
        id (d/q '[:find ?e .
                  :in $ ?pid
                  :where [?e :product/prod-id ?pid]]
                db prod-id)
        stmt (format stmt-fmt id)
        ir (stmt->ir stmt)
        _got (rtr/run-retract *conn* db ir {:silent true})
        db' (d/db *conn*)
        e->m' (partial entity->map db')
        product' (e->m' id)
        attrs' (->> product' keys (into #{}))]
    (is (= (get attrs' :product/uuid :nope) :nope)
        "the retracted attr is no longer present on target product")
    (is (= (count-entities db' :product/uuid)
           (dec product-cnt))
        "number of products with uuid attr decreased by one")
    (is (= (count-entities db' :product/prod-id)
           product-cnt)
        "number of products remains unchanged")))

(deftest retract-product-attrs-for-many-db-ids
  (let [stmt "retract #attr :product/actor,
                      #attr :product/rating,
                      #attr :product/url
                where db.id "
        db *db*
        e->m (partial entity->map db)
        retracted-attrs [:product/actor :product/rating :product/url]
        product-cnt (count-entities db :product/prod-id)
        ids (d/q '[:find [?e ...]
                   :where
                   [?e :product/prod-id ?pid]
                   [(> ?pid 6000)]]
                 db)
        remain-ids (d/q '[:find [?e ...]
                          :where
                          [?e :product/prod-id ?pid]
                          [(<= ?pid 6000)]]
                        db)
        remain-products (->> remain-ids (map e->m))
        all-attrs (->> remain-products first keys (into #{}))
        kept-attrs (set/difference all-attrs (into #{} retracted-attrs))
        stmt' (str stmt (str/join " " ids))
        ir (stmt->ir stmt')
        _got (rtr/run-retract *conn* db ir {:silent true})
        db' (d/db *conn*)
        e->m' (partial entity->map db')
        products' (->> ids (map e->m'))
        remain-products' (->> remain-ids (map e->m'))]
    (is (= (count-entities db' :product/prod-id)
           product-cnt)
        "number of products remains unchanged")
    (is (= (into #{} remain-products')
           (into #{} remain-products))
        "the non-targeted products remain unchanged")
    (is (every? (fn [p]
                  (let [attrs (->> p keys (into #{}))]
                    (= kept-attrs attrs)))
                products')
        "the retracted attrs are no longer present on target products")))

(comment

  (defn pp-ent [eid]
    (db-fixture
     (fn []
       (->> (entity->map *db* eid)
            clojure.pprint/pprint))))
  (pp-ent [:product/prod-id 9990])

  )
