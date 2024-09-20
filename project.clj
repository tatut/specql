(defproject io.github.tatut/specql "20240920"
  :description "PostgreSQL spec query language"
  :license {:name "MIT License"}
  :url "https://tatut.github.io/specql/"
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/java.jdbc "0.7.12"]]
  :profiles {:dev {:dependencies [[org.clojure/test.check "1.1.1"]
                                  [io.zonky.test/embedded-postgres "2.0.3"]]}}

  :plugins [[lein-codox "0.10.8"]]
  :source-paths ["src"]
  :test-paths ["test"]
  :jvm-opts ["-Duser.timezone=GMT"]
  :monkeypatch-clojure-test false

  :codox {:output-path "docs/api"})
