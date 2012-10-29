# A Common Crawl Experiment

## Introduction ##

At my company, we are building infrastructure that enables us to
perform computations involving large bodies of text data.

To get familiar with the tech involved, I started with a simple
experiment: using [Common Crawl metadata corpus][1], count crawled
URLs grouped by top level domain (TLD).

Here is a sample of how output from this "TLD query" might look like:

```
com     19298
org     2595
net     2228
uk      1674
de      1227
...
```

It's not a very exciting query. To be blunt, it's a pretty boring one.
But that's the point: this is a new ground for me, so I start simple,
and capture what I've learned.

This gist is a fully-fledged Git repo with all the code necessary to
run this query, so if you want to play with the code yourself, go
ahead clone this thing. You'll also need to install [Leiningen.][9]

[OS X Homebrew][10] users, here's a shortcut for you: `brew install --devel leiningen`

## Results

I've downloaded the job output files (`part-00000` ... `part-00261`)
and sorted them by TLD counts:

```sh
cat part* | sort -k2nr > results.txt
```

Here are top 10 results:

```
com     2673598481
org     304265520
net     267384802
de      205656928
uk      168846878
pl      82716496
ru      76587934
info    73012083
nl      63865755
fr      57607765
```

All results <a href="#file_results.txt">are here.</a>

### Hadoop Cluster Details

I ran this query on [Amazon EMR][7] cluster of 50 `cc1.4xlarge`
machines, which took about <a href="http://goo.gl/R9ueT"
target="_blank">80 minutes to execute</a> against entire corpus of
Common Crawl metadata. Overall, the query saw **4,781,766,325**
crawled URLs that had domain names with valid TLDs.

I also was able to confirm that simple linear equations can predict
how long a job would run based on results of the same job that was run
against a smaller data corpus and with fewer data processing nodes. I
ran it against 1 Common Crawl segment, and then against 10 segments,
and finally against all 56 segments.

Here are the monitoring graphs for the time period when the cluster
was active:
<img src="http://goo.gl/7lBZX" />

Here's the command to launch a cluster on EMR similar to what is
described here (using [Amazon EMR Command Line Interface][8]):

```sh
./elastic-mapreduce --create --name "CommonCrawl (TLD: all segments)" \
--jar s3://your.bucket/ccooo-standalone.jar \
--args 'ccooo.commoncrawl.TldQueryExe,s3://aws-publicdatasets/common-crawl/parse-output/segment,s3://your.bucket/tlds/all' \
--instance-group master --instance-type m2.2xlarge  --instance-count 1  --bid-price 0.111 \
--instance-group core   --instance-type cc1.4xlarge --instance-count 50 --bid-price 0.301 \
--bootstrap-action s3://elasticmapreduce/bootstrap-actions/configurations/latest/memory-intensive \
--availability-zone us-east-1b
```

**NOTE:** the master node needs to be configured with
`memory-intensive` bootstrap script because various JVM processes need
a lot of heap space to deal with 245,000+ Hadoop tasks.

I picked `us-east-1b` zone because at that time spot prices looked the
lowest there.

`cc1.4xlarge` nodes were chosen because, well, they are badass!
Specifically, they have 10G Ethernet NICs which is good when you're
about slurp gobs of data from S3.

### Spot Instances Are Awesome

The cluster, described above, consisted of 1 master node `m2.2xlarge`
spot priced $0.07 per hour plus EMR hourly fee of $0.21, and 50 core
nodes `cc1.4xlarge` spot priced $0.21 per hour plus EMR hourly fee of
$0.27.

Which means that for 80 minutes (rounded up to 2 hours), this works
out to:

`cost = 2 * (1 * (0.07 + 0.21)  + 50 * (0.21 + 0.27)) = $48.56`

Not too expensive! Let me know if I botched my math. I haven't seen a
drastic slope increase in our AWS billing curve, so I can't be that
far off.

## Code

I've implemented the TLD Query using [Cascalog,][2] which strikes me
as one of the most eloquent ways to express Hadoop map/reduce logic.

Let's dive in.

Common Crawl metadata files are organized into segment directories,
each containing several thousand files named `metadata-NNNNN`. As it
turns out, not all segments are ready for consumption. If you naively
use a glob like this
`s3://aws-publicdatasets/common-crawl/parse-output/segment/*/metadata*`,
your Hadoop job will crash when trying to iterate/read S3 objects
using this glob, because some objects are not public. This manifests
itself as `... Status Code: 403, AWS Service: Amazon S3...` error.

Common Crawl team publishes [a file][3] containing segment IDs that
are valid. Rationale for using this approach is [discussed here.][4]

Lesson 1. Use a hand-crafted glob with valid segments only.
`metadata-path` function, shown below, produces such a glob based on
hard-coded vector `valid-segments`.

```clojure
(def valid-segments [1346823845675 ... 54 more elided ... 1346981172268])

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
```

The resulting glob will look like follows (assuming you specified
prefix `s3://aws-publicdatasets/common-crawl/parse-output/segment`).

```
"s3://aws-publicdatasets/common-crawl/parse-output/segment/{seg1,seg2,seg3,...}/metadata*"
```

Moving on.

Each metadata file contains a sequence of records each comprised of 2
items: crawled URL and metadata JSON. This query does not care about
metadata JSON, it only cares about the hostname in each URL. The
following bits of Clojure take care of parsing TLD of the URL's
hostname (assuming it's a domain name).

`valid-tld` set will be used by the query to check whether a string is
in fact a TLD.

Look at <a href="#file_ccooo/test/ccooo/test_ccooo.clj">the test code</a>
to see `parse-tld` in action.

```clojure
;; A set of valid TLDs obtained from:
;; http://data.iana.org/TLD/tlds-alpha-by-domain.txt
(def valid-tld #{"ac" ... "com" ... "zw"})

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
```

Now let's look at the Cascalog query, `query-tlds` that does all the
work.

The bit right after `<-` specifies that the query produces a
sequence of tuples `[?url ?n]` (e.g. `[com 3]`, `[uk 77]`, `[net 2]`).

Then it specifies that it only cares about `?url` values coming out
from the source tap `metadata-tap.` (remember: each metadata tuple
consists of two values: crawled URL and metadata JSON; the second
value is not important, so we indicate that with `_` placeholder).

Each `?url` value is parsed into `?tld` value using `parse-tld`
function we discussed earlier.

Predicate `(valid-tld ?tld)` checks if `?tld` is valid. If result is
`nil`, the `?url` in question will not count.

Aggregator `(c/count :> ?n)` counts crawled `?url-s` grouped by
`?tld.`

When your Hadoop job is about 73% through several billion URLs, you
wouldn't want it to be aborted because of some malformed tuples, would
you? That's why `(:trap ...)` is specified: it traps tuple values that
cause exceptions and sends them to `trap-tap` sink.

```clojure
(defn query-tlds
  "Counts site URLs from the metadata corpus grouped by TLD of each URL."
  [metadata-tap trap-tap]
  (<- [?tld ?n]
      (metadata-tap :> ?url _)
      (parse-tld ?url :> ?tld)
      (valid-tld ?tld)
      (c/count :> ?n)
      (:trap trap-tap)))
```

All Hadoop applications need to expose one or more entry point classes
that have `main` method that defines pertinent job details. In
Cascalog, this is done without much fuss using `defmain`.

The tricky part: I discovered I can't use the default `hfs-seqfile`
source tap for processing Common Crawl metadata files. It turns out,
Cascalog's `hfs-seqfile` creates a `SequenceFile` [Cascading][5] tap.
Cascading sequence files store a sequence of serialized Cascading
`Tuple` objects. Common Crawl sequence files store a sequence of
serialized key/value pairs (both key and value are of
`org.apache.hadoop.io Text` type). So initially this query crashed
with serialization errors when it touched Common Crawl data. I found
that I needed to use a `WritableSequenceFile` source tap instead.
Handily, [more-taps][6] library already provides such a tap creation
helper: `hfs-wrtseqfile`. And this was Lesson 2.

```clojure
(defmain TldQueryExe
  "Defines 'main' method that will execute our query."
  [prefix output-dir]
  (let [metadata-tap (hfs-wrtseqfile (metadata-path prefix) Text Text :outfields ["key" "value"])
        trap-tap (hfs-seqfile (str output-dir ".trap"))]
    (?- (hfs-textline output-dir)
        (query-tlds metadata-tap trap-tap))))
```

You may invoke `TldQueryExe.main` entry point from command line like
so:

```sh
hadoop jar ccooo-standalone.jar ccooo.commoncrawl.TldQueryExe \
 s3://aws-publicdatasets/common-crawl/parse-output/segment \
 s3://bucket-you-own/some/path
```

FYI, malformed tuples will automatically be written to
`s3://bucket-you-own/some/path.trap` due to `trap-tap (hfs-seqfile (str output-dir ".trap"))` incantation above.

To run locally, you may want to download a couple metadata files and
then use local file system paths instead of `s3://` URLs. Say, if you
have one metadata file at this path: `~/Downloads/1346823845675/metadata-00000` 
And you want to write results to `./tld-results`, the command line
invocation would be:

```
hadoop jar ccooo-standalone.jar ccooo.commoncrawl.TldQueryExe  ~/Downloads ./tld-results
```

[1]: https://commoncrawl.atlassian.net/wiki/display/CRWL/About+the+Data+Set
[2]: http://nathanmarz.com/blog/introducing-cascalog-a-clojure-based-query-language-for-hado.html
[3]: http://s3.amazonaws.com/aws-publicdatasets/common-crawl/parse-output/valid_segments.txt
[4]: https://groups.google.com/forum/#!topic/common-crawl/QYTmnttZZyo/discussion
[5]: http://docs.cascading.org/cascading/2.0/userguide/htmlsingle/#N20867
[6]: https://github.com/nathanmarz/cascalog-contrib/tree/master/cascalog.more-taps
[7]: http://aws.amazon.com/elasticmapreduce/
[8]: http://docs.amazonwebservices.com/ElasticMapReduce/latest/GettingStartedGuide/SignUp.html
[9]: https://github.com/technomancy/leiningen/blob/master/README.md
[10]: http://mxcl.github.com/homebrew/
