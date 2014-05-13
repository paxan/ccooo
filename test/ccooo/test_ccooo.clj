(ns ccooo.test-ccooo
  (:require [cascalog.logic.testing :refer :all]
            [ccooo.commoncrawl :as commoncrawl]
            [clojure.test :refer :all]
            [midje.cascalog :refer :all]
            [midje.sweet :refer :all])
  (:import (org.apache.hadoop.io Text)))


(deftest commoncrawl-tests
  (fact "Hostname parsing"
    (-> "http://zorg.me/give/me/the/stones?q=foo bar" Text. commoncrawl/parse-hostname) => "zorg.me"
    (-> "" Text. commoncrawl/parse-hostname) => ""
    (-> "()" Text. commoncrawl/parse-hostname) => ""
    (-> "abc" Text. commoncrawl/parse-hostname) => ""
    (-> "what://wsx/qaz?q=a%20b" Text. commoncrawl/parse-hostname) => "wsx")

  (fact "TLD parsing"
    (-> "http://foo.bar.baz/1/2/3.txt" Text. commoncrawl/parse-tld) => "baz"
    (-> "https://oh.really.xn--fiqs8s:443/yeah" Text. commoncrawl/parse-tld) => "xn--fiqs8s")

  (with-expected-sink-sets [nothing-trapped []]
    (let [metadata-tap [["http://abc.example.com/123" "{}"]
                        ["http://foo.bogus/" "{}"] ;; this one should be skipped!
                        ["http://xyz.example.com/890" "{}"]
                        ["https://wow.co.uk/hello" "{}"]]]
      (fact "the TLD query works all right"
        (commoncrawl/query-tlds metadata-tap nothing-trapped) =>
        (produces [["com" 2] ["uk" 1]])))))
