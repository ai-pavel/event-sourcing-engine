(defproject event-sourcing-engine "0.1.0"
  :description "A Clojure event sourcing framework with SQLite-backed event store"
  :license {:name "MIT"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [com.github.seancorfield/next.jdbc "1.3.909"]
                 [org.xerial/sqlite-jdbc "3.45.1.0"]
                 [org.clojure/data.json "2.4.0"]]
  :main event-sourcing.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
