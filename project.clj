(defproject tesser.spark "1.0.2"
  :description "Tesser: Spark support via Flambo."
  :url "http://github.com/aphyr/tesser"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :java-source-paths ["src/"]
  :javac-options ["-target" "1.5"
                  "-source" "1.5"]
  :repositories
  {"cloudera" "https://repository.cloudera.com/artifactory/cloudera-repos/"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [tesser.core "1.0.2"]
                 [tesser.math "1.0.2"]
                 [org.clojure/data.fressian "0.2.0"]
                 [yieldbot/flambo "0.8.0"]
                 [org.clojars.achim/multiset "0.1.0-SNAPSHOT"]
                 [criterium "0.4.3"]
                 [org.clojure/test.check "0.7.0"]
                 ]
   :profiles {:dev
             ;; so gen-class stuff works in the repl
              {:aot [flambo.function]}
             :provided
             {:dependencies
              [[org.apache.spark/spark-core_2.11 "2.0.1"]
              ]}
             :uberjar
             {:aot :all}})

