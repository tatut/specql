(defproject specql/specql "0.7.0-alpha1"
  :description "PostgreSQL spec query language"
  :license {:name "MIT License"}
  :url "https://tatut.github.io/specql/"
  :dependencies [[org.clojure/clojure "1.9.0-alpha16"]
                 [org.clojure/java.jdbc "0.7.0-alpha3"]]
  :profiles {:dev {:dependencies [[org.clojure/test.check "0.9.0"]
                                 [com.opentable.components/otj-pg-embedded "0.7.1"]]}}

  :plugins [[lein-codox "0.10.3"]]
  :source-paths ["src"]
  :test-paths ["test"]
  :jvm-opts ["-Duser.timezone=GMT"]
  :monkeypatch-clojure-test false

  :codox {:output-path "docs/api"})
