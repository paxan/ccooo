(defproject org.clojars.paxan/ccooo "0.0.1-SNAPSHOT"
  :description "Common Crawl One Oh One (CCOOO)"
  :jar-name "ccooo.jar"
  :uberjar-name "ccooo-standalone.jar"
  :dependencies [[cascalog "2.1.0"]
                 [org.clojure/clojure "1.5.1"]
                 [commons-httpclient "3.0.1"]]
  :profiles {:provided
             {:dependencies [[org.apache.hadoop/hadoop-core "1.1.2"]]}
             :dev
             {:dependencies [[midje "1.5.1"]]
              :plugins [[lein-midje "3.0.1"]]}}
  :aot [ccooo.commoncrawl]
  :jvm-opts ["-Xmx1024m" "-server"])
