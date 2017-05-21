(ns tesser.spark.core-test
  (:require [clojure.test :refer :all]
            [clojure.math.numeric-tower :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check [clojure-test :refer :all]
                                [generators :as gen]
                                [properties :as prop]]
            [multiset.core :refer [multiset]]
            [tesser.utils :refer :all]
            [tesser [core :as t]
                    [math :as m]
                    [quantiles :as q]]))

