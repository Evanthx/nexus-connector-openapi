.PHONY: generate langs build test vet

# nexgen is downloaded from its GitHub release into .build/ the first time it is needed.
NEXGEN_VERSION := 0.2.9
NEXGEN_OS := $(if $(filter Darwin,$(shell uname -s)),apple-darwin,unknown-linux-gnu)
NEXGEN_ARCH := $(if $(filter arm64 aarch64,$(shell uname -m)),aarch64,x86_64)
NEXGEN := .build/nexgen-$(NEXGEN_VERSION)

$(NEXGEN):
	mkdir -p .build
	curl -sSfL https://github.com/temporalio/nexgen/releases/download/v$(NEXGEN_VERSION)/nexgen-v$(NEXGEN_VERSION)-$(NEXGEN_ARCH)-$(NEXGEN_OS).tar.gz | tar -xz -C .build nexgen
	mv .build/nexgen $@

# For each API: openapi2nexus writes the contract and the activities, then nexgen writes the Go
# types from the contract. nexgen never deletes files, so the old types are removed first.
generate: $(NEXGEN)
	go run ./cmd/openapi2nexus apis/weather/connector.yaml
	rm -rf apis/weather/weatherapi
	$(NEXGEN) go apis/weather/weather.nexusrpc.yaml --output apis/weather/weatherapi
	go run ./cmd/openapi2nexus apis/forecastreports/connector.yaml
	rm -rf apis/forecastreports/reportsapi
	$(NEXGEN) go apis/forecastreports/forecastreports.nexusrpc.yaml --output apis/forecastreports/reportsapi
	gofmt -w apis

# Generates callers in nexgen's other languages into .build/langs, to show the contracts work for
# them too. Nothing here uses them.
langs: $(NEXGEN)
	rm -rf .build/langs
	$(NEXGEN) java apis/weather/weather.nexusrpc.yaml --package-name io.temporal.connectors.weather --output .build/langs/java/weather
	$(NEXGEN) java apis/forecastreports/forecastreports.nexusrpc.yaml --package-name io.temporal.connectors.forecastreports --output .build/langs/java/forecastreports
	for lang in python typescript; do \
		$(NEXGEN) $$lang apis/weather/weather.nexusrpc.yaml --output .build/langs/$$lang/weather || exit 1; \
		$(NEXGEN) $$lang apis/forecastreports/forecastreports.nexusrpc.yaml --output .build/langs/$$lang/forecastreports || exit 1; \
	done

build:
	go build ./...

test:
	go test -race -count=1 ./...

vet:
	go vet ./...
