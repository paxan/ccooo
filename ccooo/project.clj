(defproject org.clojars.paxan/ccooo "0.0.1-SNAPSHOT"
  :description "Common Crawl One Oh One (CCOOO)"
  :jar-name "ccooo.jar"
  :uberjar-name "ccooo-standalone.jar"
  :dependencies [[cascalog "1.10.0"]
                 [cascalog-more-taps "0.3.0"]
                 [org.clojure/data.json "0.1.3"]
                 [commons-httpclient "3.0.1"]]
  :profiles {:dev
             {:dependencies [[org.apache.hadoop/hadoop-core "1.0.3"]
                             [midje "1.4.0"]
                             [midje-cascalog "0.4.0" :exclusions [org.clojure/clojure]]]}}
  :aot [ccooo.commoncrawl]
  :jvm-opts ["-Xmx1024m" "-server"])
