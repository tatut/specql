(defproject specql/specql "0.2-SNAPSHOT"
  :description "PostgreSQL spec query language"
  :dependencies [[org.clojure/clojure "1.9.0-alpha14"]
                 [org.clojure/java.jdbc "0.7.0-alpha2"]]
  :profiles {:dev {:dependencies [[org.clojure/test.check "0.9.0"]
                                 [com.opentable.components/otj-pg-embedded "0.7.1"]]}}

  :source-paths ["src"]
  :test-paths ["test"]
  :jvm-opts ["-Duser.timezone=GMT"]
  :monkeypatch-clojure-test false)
