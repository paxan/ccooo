# Collect a list of all Clojure source code file names.
sources := $(shell find . -type f -name "*.clj" -not -wholename "*/target/*")

target/ccooo-standalone.jar: $(sources)
	lein do compile, uberjar

.PHONY: test
test:
	lein do compile, midje
