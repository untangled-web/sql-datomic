(ns sql-datomic.datomic
  (:require [datomic.api :as d]
            [clojure.edn :as edn]))

(def default-connection-uri "datomic:mem://dellstore")

(defn read-schema-tx []
  (-> "resources/dellstore-schema.edn"
      slurp
      read-string))

(def customers-tx
  [
   ;; orderid 2
   {:db/id #db/id[:db.part/user]
    :customer/creditcard "9752442379947752"
    :customer/income 20000M
    :customer/email "OVWOIYIDDL@dell.com"
    :customer/lastname "OVWOIYIDDL"
    :customer/phone "6298730047"
    :customer/password "password"
    :customer/age 56
    :customer/creditcardexpiration "2010/04"
    :customer/state "IA"
    :customer/city "IQIDDJY"
    :customer/creditcardtype 5
    :customer/zip "98082"
    :customer/username "user4858"
    :customer/address-1 "6298730047 Dell Way"
    :customer/region 1
    :customer/firstname "SKULRB"
    :customer/customerid 4858
    :customer/country "US"
    :customer/gender :customer.gender/male}

   ;; orderid 5
   {:customer/creditcard "2564112400636077"
    :customer/income 40000M
    :customer/email "JSCBIDJQER@dell.com"
    :customer/lastname "JSCBIDJQER"
    :customer/phone "6447516578"
    :customer/password "password"
    :customer/age 44
    :customer/creditcardexpiration "2009/10"
    :customer/city "KEFQJES"
    :customer/creditcardtype 3
    :customer/zip "0"
    :customer/username "user14771"
    :customer/address-1 "6447516578 Dell Way"
    :customer/region 2
    :customer/firstname "JIDXLK"
    :db/id #db/id[:db.part/user]
    :customer/customerid 14771
    :customer/country "Canada"
    :customer/gender :customer.gender/female}])

(def products-tx
  [
   ;; orderid 2
   {:db/id #db/id[:db.part/user]
    :product/prod-id 6127
    :product/category :product.category/games
    :product/title "AIRPLANE CAT"
    :product/actor "RIP DOUGLAS"
    :product/price 16.99M
    :product/special false
    :product/common-prod-id 1644}
   {:db/id #db/id[:db.part/user]
    :product/prod-id 4936
    :product/category :product.category/new
    :product/title "AFRICAN VANISHING"
    :product/actor "EDWARD NOLTE"
    :product/price 16.99M
    :product/special false
    :product/common-prod-id 4633}
   {:db/id #db/id[:db.part/user]
    :product/prod-id 1298
    :product/category :product.category/comedy
    :product/title "ACE EYES"
    :product/actor "DENNIS FIENNES"
    :product/price 26.99M
    :product/special false
    :product/common-prod-id 5006}
   {:db/id #db/id[:db.part/user]
    :product/prod-id 2926
    :product/category :product.category/music
    :product/title "ADAPTATION UNTOUCHABLES"
    :product/actor "HELEN BERRY"
    :product/price 22.99M
    :product/special false
    :product/common-prod-id 2561}
   {:db/id #db/id[:db.part/user]
    :product/prod-id 5130
    :product/category :product.category/action
    :product/title "AGENT CELEBRITY"
    :product/actor "ORLANDO KILMER"
    :product/price 14.99M
    :product/special false
    :product/common-prod-id 8074}
   {:db/id #db/id[:db.part/user]
    :product/prod-id 9990
    :product/category :product.category/music
    :product/title "ALADDIN WORLD"
    :product/actor "HUMPHREY DENCH"
    :product/price 25.99M
    :product/special false
    :product/common-prod-id 6584}
   {:db/id #db/id[:db.part/user]
    :product/prod-id 1567
    :product/category :product.category/new
    :product/title "ACE MEET"
    :product/actor "JESSICA PFEIFFER"
    :product/price 25.99M
    :product/special false
    :product/common-prod-id 5179}
   {:db/id #db/id[:db.part/user]
    :product/prod-id 6376
    :product/category :product.category/drama
    :product/title "AIRPLANE GRAPES"
    :product/actor "KIM RYDER"
    :product/price 21.99M
    :product/special false
    :product/common-prod-id 7411}

   ;; orderid 5
   {:db/id #db/id[:db.part/user]
    :product/prod-id 8293
    :product/category :product.category/family
    :product/title "ALABAMA EXORCIST"
    :product/actor "NICK MINELLI"
    :product/price 13.99M
    :product/special false
    :product/common-prod-id 4804}
   {:db/id #db/id[:db.part/user]
    :product/prod-id 4402
    :product/category :product.category/children
    :product/title "AFRICAN HARPER"
    :product/actor "GENE WILLIS"
    :product/price 11.99M
    :product/special false
    :product/common-prod-id 1261}
   {:db/id #db/id[:db.part/user]
    :product/prod-id 6879
    :product/category :product.category/action
    :product/title "AIRPLANE TELEGRAPH"
    :product/actor "LIAM KILMER"
    :product/price 12.99M
    :product/special false
    :product/common-prod-id 6893}
   {:db/id #db/id[:db.part/user]
    :product/prod-id 2290
    :product/category :product.category/documentary
    :product/title "ADAPTATION EVERYONE"
    :product/actor "ANNETTE BEATTY"
    :product/price 15.99M
    :product/special false
    :product/common-prod-id 2928}])

(def orders-tx
  [{:db/id #db/id[:db.part/user]
    :order/orderid 2
    :order/tax 4.53M
    :order/netamount 54.90M
    :order/customerid 4858
    :order/customer [:customer/customerid 4858]
    :order/orderlines
    #{{:db/id #db/id[:db.part/user]
       :orderline/orderlineid 6
       :orderline/orderid 2
       :orderline/prod-id 6376
       :orderline/product [:product/prod-id 6376]
       :orderline/quantity 2
       :orderline/orderdate #inst "2004-01-01T08:00:00.000-00:00"}
      {:db/id #db/id[:db.part/user]
       :orderline/orderlineid 5
       :orderline/orderid 2
       :orderline/prod-id 6127
       :orderline/product [:product/prod-id 6127]
       :orderline/quantity 1
       :orderline/orderdate #inst "2004-01-01T08:00:00.000-00:00"}
      {:db/id #db/id[:db.part/user]
       :orderline/orderlineid 3
       :orderline/orderid 2
       :orderline/prod-id 9990
       :orderline/product [:product/prod-id 9990]
       :orderline/quantity 1
       :orderline/orderdate #inst "2004-01-01T08:00:00.000-00:00"}
      {:db/id #db/id[:db.part/user]
       :orderline/orderlineid 4
       :orderline/orderid 2
       :orderline/prod-id 5130
       :orderline/product [:product/prod-id 5130]
       :orderline/quantity 3
       :orderline/orderdate #inst "2004-01-01T08:00:00.000-00:00"}
      {:db/id #db/id[:db.part/user]
       :orderline/orderlineid 1
       :orderline/orderid 2
       :orderline/prod-id 1567
       :orderline/product [:product/prod-id 1567]
       :orderline/quantity 2
       :orderline/orderdate #inst "2004-01-01T08:00:00.000-00:00"}
      {:db/id #db/id[:db.part/user]
       :orderline/orderlineid 2
       :orderline/orderid 2
       :orderline/prod-id 1298
       :orderline/product [:product/prod-id 1298]
       :orderline/quantity 1
       :orderline/orderdate #inst "2004-01-01T08:00:00.000-00:00"}
      {:db/id #db/id[:db.part/user]
       :orderline/orderlineid 8
       :orderline/orderid 2
       :orderline/prod-id 2926
       :orderline/product [:product/prod-id 2926]
       :orderline/quantity 3
       :orderline/orderdate #inst "2004-01-01T08:00:00.000-00:00"}
      {:db/id #db/id[:db.part/user]
       :orderline/orderlineid 7
       :orderline/orderid 2
       :orderline/prod-id 4936
       :orderline/product [:product/prod-id 4936]
       :orderline/quantity 3
       :orderline/orderdate #inst "2004-01-01T08:00:00.000-00:00"}}
    :order/totalamount 59.43M
    :order/orderdate #inst "2004-01-01T08:00:00.000-00:00"}

   {:db/id #db/id[:db.part/user]
    :order/orderid 5
    :order/tax 21.12M
    :order/netamount 256.00M
    :order/customerid 14771
    :order/customer [:customer/customerid 14771]
    :order/orderlines
    #{{:db/id #db/id[:db.part/user]
       :orderline/orderlineid 3
       :orderline/orderid 5
       :orderline/prod-id 8293
       :orderline/product [:product/prod-id 8293]
       :orderline/quantity 1
       :orderline/orderdate #inst "2004-01-09T08:00:00.000-00:00"}
      {:db/id #db/id[:db.part/user]
       :orderline/orderlineid 2
       :orderline/orderid 5
       :orderline/prod-id 4402
       :orderline/product [:product/prod-id 4402]
       :orderline/quantity 3
       :orderline/orderdate #inst "2004-01-09T08:00:00.000-00:00"}
      {:db/id #db/id[:db.part/user]
       :orderline/orderlineid 4
       :orderline/orderid 5
       :orderline/prod-id 2290
       :orderline/product [:product/prod-id 2290]
       :orderline/quantity 3
       :orderline/orderdate #inst "2004-01-09T08:00:00.000-00:00"}
      {:db/id #db/id[:db.part/user]
       :orderline/orderlineid 1
       :orderline/orderid 5
       :orderline/prod-id 6879
       :orderline/product [:product/prod-id 6879]
       :orderline/quantity 1
       :orderline/orderdate #inst "2004-01-09T08:00:00.000-00:00"}}
    :order/totalamount 277.12M
    :order/orderdate #inst "2004-01-09T08:00:00.000-00:00"}
   ])

(defn create-default-db []
  (d/create-database default-connection-uri)
  (let [connection (d/connect default-connection-uri)
        schema-tx (read-schema-tx)]
    @(d/transact connection schema-tx)
    @(d/transact connection customers-tx)
    @(d/transact connection products-tx)
    @(d/transact connection orders-tx)
    connection))

(defn delete-default-db []
  (d/delete-database default-connection-uri))

(defn recreate-default-db []
  (delete-default-db)
  (create-default-db))
