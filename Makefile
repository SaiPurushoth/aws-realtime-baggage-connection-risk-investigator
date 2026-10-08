SHELL := /bin/bash
NODE24_BIN := /opt/homebrew/opt/node@24/bin
JSII_CACHE := $(CURDIR)/.cache/jsii
CDK_ENV := PATH="$(NODE24_BIN):$$PATH" JSII_RUNTIME_PACKAGE_CACHE_ROOT="$(JSII_CACHE)"

.PHONY: help layout validate test dashboard package-flink synth diff \
	platform-start platform-stop platform-status scenario-normal scenario-isolated scenario-zone scenario-delay \
	stack-create stack-destroy stack-recreate

STACK_CONFIRM ?=

help:
	@echo "BagGuard repository commands"
	@echo "  make layout    Show the repository structure"
	@echo "  make validate  Verify the required scaffold"
	@echo "  make test      Run Python unit-test discovery"
	@echo "  make dashboard Start the local BagGuard Operations Console"
	@echo "  make platform-start Start and verify BagGuard compute"
	@echo "  make platform-stop  Gracefully stop BagGuard compute"
	@echo "  make platform-status Show BagGuard runtime status"
	@echo "  make stack-create Deploy every stack, start compute, and verify"
	@echo "  make stack-destroy STACK_CONFIRM=<token> Permanently delete BagGuard"
	@echo "  make stack-recreate STACK_CONFIRM=<token> Delete and rebuild in one run"
	@echo "  make scenario-normal Run a normal connection scenario"
	@echo "  make scenario-isolated Run an isolated bag stall scenario"
	@echo "  make scenario-zone Run a transfer-zone congestion scenario"
	@echo "  make scenario-delay Run an inbound flight delay scenario"
	@echo "  make package-flink  Build the Managed Flink deployment ZIP"
	@echo "  make synth     Synthesize the CDK template"
	@echo "  make diff      Show the CDK diff without a change set"

layout:
	@find . -maxdepth 3 -not -path './.git*' | sort

validate:
	@for path in infra simulator flink lambdas/clickhouse_adapter lambdas/dispatcher agent dashboard tests scripts; do \
		test -d "$$path" || { echo "Missing directory: $$path"; exit 1; }; \
	done
	@test -f README.md
	@test -f AGENTS.md
	@test -f .env.example
	@test -f .gitignore
	@echo "Repository scaffold is valid."

test:
	@if test -n "$$(find tests -type f -name 'test_*.py' -print -quit)"; then \
		mkdir -p "$(JSII_CACHE)"; \
		$(CDK_ENV) .venv/bin/python -m unittest discover -s tests -p 'test_*.py'; \
	else \
		echo "No tests exist yet; application implementation has not started."; \
	fi

dashboard:
	@AWS_REGION=us-east-1 \
	BAGGUARD_EVENT_STREAM_NAME=bagguard-baggage-events-prod \
	BAGGUARD_CLICKHOUSE_ADAPTER_FUNCTION_NAME=bagguard-clickhouse-adapter-prod \
	PYTHONPATH="$(CURDIR)" \
	.venv/bin/streamlit run dashboard/app.py

platform-start:
	@scripts/platform-start.sh

platform-stop:
	@scripts/platform-stop.sh

platform-status:
	@scripts/platform-status.sh

stack-create:
	@scripts/stack-create.sh

stack-destroy:
	@scripts/stack-destroy.sh --confirm "$(STACK_CONFIRM)"

stack-recreate:
	@scripts/stack-recreate.sh --confirm "$(STACK_CONFIRM)"

scenario-normal:
	@.venv/bin/python simulator/producer.py --scenario normal

scenario-isolated:
	@.venv/bin/python simulator/producer.py --scenario isolated-stall

scenario-zone:
	@.venv/bin/python simulator/producer.py --scenario zone-congestion

scenario-delay:
	@.venv/bin/python simulator/producer.py --scenario inbound-delay

package-flink:
	@scripts/package_flink.sh

synth: package-flink
	@mkdir -p "$(JSII_CACHE)"
	@$(CDK_ENV) cdk synth

diff:
	@mkdir -p "$(JSII_CACHE)"
	@$(CDK_ENV) cdk diff --no-change-set
