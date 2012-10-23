(ns ccooo.test-ccooo
  (:use [midje sweet cascalog]
        ccooo.commoncrawl
        cascalog.api
        cascalog.testing)
  (:import [org.apache.hadoop.io Text]))

(fact "Hostname parsing"
      (-> "http://zorg.me/give/me/the/stones?q=foo bar" Text. parse-hostname) => "zorg.me"
      (-> "" Text. parse-hostname) => ""
      (-> "()" Text. parse-hostname) => ""
      (-> "abc" Text. parse-hostname) => ""
      (-> "what://wsx/qaz?q=a%20b" Text. parse-hostname) => "wsx")

(fact "TLD parsing"
      (-> "http://foo.bar.baz/1/2/3.txt" Text. parse-tld) => "baz"
      (-> "https://oh.really.xn--fiqs8s:443/yeah" Text. parse-tld) => "xn--fiqs8s")

(with-expected-sink-sets [nothing-trapped []]
  (let [metadata-tap [["http://abc.example.com/123" "{}"]
                      ["http://foo.bogus/" "{}"] ;; this one should be skipped!
                      ["http://xyz.example.com/890" "{}"]
                      ["https://wow.co.uk/hello" "{}"]]]
   (fact?- "the TLD query works all right"
           [["com" 2]
            ["uk" 1]]
           (query-tlds metadata-tap nothing-trapped))))
