(ns ccooo.commoncrawl
  (:require [cascalog.api :refer :all]
            [cascalog.logic.ops :as c]
            [cascalog.more-taps :refer [hfs-wrtseqfile]]
            [clojure.string :as s])
  (:import (org.apache.commons.httpclient URI)
           (org.apache.hadoop.io Text)))

;; Discussion about valid segments:
;; https://groups.google.com/forum/#!topic/common-crawl/QYTmnttZZyo/discussion
;;
;; I was led to this discussion because when naively globbing accross
;; all segments, my hadoop job failed with: 2012-10-10 21:44:09,895
;; INFO org.apache.hadoop.io.retry.RetryInvocationHandler
;; (pool-1-thread-1): Exception while invoking retrieveMetadata of
;; class org.apache.hadoop.fs.s3native.Jets3tNativeFileSystemStore.
;; Not retrying.Status Code: 403, AWS Service: Amazon S3, AWS Request
;; ID: ..., AWS Error Code: null, AWS Error Message: Forbidden, S3
;; Extended Request ID: ...
;;
;; I found this discussion related to such errors:
;; https://groups.google.com/d/topic/common-crawl/e07aej71GLU/discussion

;; Obtained from:
;; https://s3.amazonaws.com/aws-publicdatasets/common-crawl/parse-output/valid_segments.txt
(def valid-segments [1346823845675 1346823846036 1346823846039 1346823846110 1346823846125 1346823846150 1346823846176 1346876860445 1346876860454 1346876860467 1346876860493 1346876860565 1346876860567 1346876860596 1346876860609 1346876860611 1346876860614 1346876860648 1346876860765 1346876860767 1346876860774 1346876860777 1346876860779 1346876860782 1346876860786 1346876860789 1346876860791 1346876860795 1346876860798 1346876860804 1346876860807 1346876860817 1346876860819 1346876860828 1346876860835 1346876860838 1346876860840 1346876860843 1346876860877 1346981172137 1346981172142 1346981172155 1346981172184 1346981172186 1346981172229 1346981172231 1346981172234 1346981172239 1346981172250 1346981172253 1346981172255 1346981172258 1346981172261 1346981172264 1346981172266 1346981172268])

;; A set of valid TLDs obtained from:
;; http://data.iana.org/TLD/tlds-alpha-by-domain.txt
(def valid-tld #{"ac" "ad" "ae" "aero" "af" "ag" "ai" "al" "am" "an" "ao" "aq" "ar" "arpa" "as" "asia" "at" "au" "aw" "ax" "az" "ba" "bb" "bd" "be" "bf" "bg" "bh" "bi" "biz" "bj" "bm" "bn" "bo" "br" "bs" "bt" "bv" "bw" "by" "bz" "ca" "cat" "cc" "cd" "cf" "cg" "ch" "ci" "ck" "cl" "cm" "cn" "co" "com" "coop" "cr" "cu" "cv" "cw" "cx" "cy" "cz" "de" "dj" "dk" "dm" "do" "dz" "ec" "edu" "ee" "eg" "er" "es" "et" "eu" "fi" "fj" "fk" "fm" "fo" "fr" "ga" "gb" "gd" "ge" "gf" "gg" "gh" "gi" "gl" "gm" "gn" "gov" "gp" "gq" "gr" "gs" "gt" "gu" "gw" "gy" "hk" "hm" "hn" "hr" "ht" "hu" "id" "ie" "il" "im" "in" "info" "int" "io" "iq" "ir" "is" "it" "je" "jm" "jo" "jobs" "jp" "ke" "kg" "kh" "ki" "km" "kn" "kp" "kr" "kw" "ky" "kz" "la" "lb" "lc" "li" "lk" "lr" "ls" "lt" "lu" "lv" "ly" "ma" "mc" "md" "me" "mg" "mh" "mil" "mk" "ml" "mm" "mn" "mo" "mobi" "mp" "mq" "mr" "ms" "mt" "mu" "museum" "mv" "mw" "mx" "my" "mz" "na" "name" "nc" "ne" "net" "nf" "ng" "ni" "nl" "no" "np" "nr" "nu" "nz" "om" "org" "pa" "pe" "pf" "pg" "ph" "pk" "pl" "pm" "pn" "post" "pr" "pro" "ps" "pt" "pw" "py" "qa" "re" "ro" "rs" "ru" "rw" "sa" "sb" "sc" "sd" "se" "sg" "sh" "si" "sj" "sk" "sl" "sm" "sn" "so" "sr" "st" "su" "sv" "sx" "sy" "sz" "tc" "td" "tel" "tf" "tg" "th" "tj" "tk" "tl" "tm" "tn" "to" "tp" "tr" "travel" "tt" "tv" "tw" "tz" "ua" "ug" "uk" "us" "uy" "uz" "va" "vc" "ve" "vg" "vi" "vn" "vu" "wf" "ws" "xn--0zwm56d" "xn--11b5bs3a9aj6g" "xn--3e0b707e" "xn--45brj9c" "xn--80akhbyknj4f" "xn--80ao21a" "xn--90a3ac" "xn--9t4b11yi5a" "xn--clchc0ea0b2g2a9gcd" "xn--deba0ad" "xn--fiqs8s" "xn--fiqz9s" "xn--fpcrj9c3d" "xn--fzc2c9e2c" "xn--g6w251d" "xn--gecrj9c" "xn--h2brj9c" "xn--hgbk6aj7f53bba" "xn--hlcj6aya9esc7a" "xn--j6w193g" "xn--jxalpdlp" "xn--kgbechtv" "xn--kprw13d" "xn--kpry57d" "xn--lgbbat1ad8j" "xn--mgb9awbf" "xn--mgbaam7a8h" "xn--mgbayh7gpa" "xn--mgbbh1a71e" "xn--mgbc0a9azcg" "xn--mgberp4a5d4ar" "xn--mgbx4cd0ab" "xn--o3cw4h" "xn--ogbpf8fl" "xn--p1ai" "xn--pgbs0dh" "xn--s9brj9c" "xn--wgbh1c" "xn--wgbl6a" "xn--xkc2al3hye2a" "xn--xkc2dl3a5ee0h" "xn--yfro4i67o" "xn--ygbi2ammx" "xn--zckzah" "xxx" "ye" "yt" "za" "zm" "zw"})

(defn ^String metadata-path
  "Produces the glob of paths to metadata files organized
   according to CommonCrawl docs: http://tinyurl.com/common-crawl-about-dataset
   Here's an example (assuming valid segments are 1, 2 and 3):
   (metadata-path 's3://path/to/commoncrawl/segments')
   => 's3://path/to/commoncrawl/segments/{1,2,3}/metadata*'"
  [prefix]
  (->> valid-segments
       (s/join ",")
       (format "%s/{%s}/metadata*" prefix)))

(defn ^String parse-hostname
  "Extracts the hostname from the specified URL."
  [^Text url]
  (-> url str URI. .getHost (or "")))

(defn ^String parse-tld
  "Returns a piece of the URL's hostname after the last '.'
   Note: this may or not be an actual TLD, we'll validate
   eslewhere."
  [^Text url]
  (-> url parse-hostname (s/split #"\.") peek))

(defn query-tlds
  "Counts site URLs from the metadata corpus grouped by TLD of each URL."
  [metadata-tap trap-tap]
  (<- [?tld ?n]
      (metadata-tap :> ?url _)
      (parse-tld ?url :> ?tld)
      (valid-tld :< ?tld)
      (c/count :> ?n)
      (:trap trap-tap)))

(defmain TldQueryExe
  "Defines 'main' method that will execute our query."
  [prefix output-dir]
  (let [metadata-tap (hfs-wrtseqfile (metadata-path prefix)
                                     Text Text
                                     :outfields ["key" "value"])
        trap-tap (hfs-seqfile (str output-dir ".trap"))]
    (?- (hfs-textline output-dir)
        (query-tlds metadata-tap trap-tap))))
