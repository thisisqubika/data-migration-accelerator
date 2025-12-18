SHELL := /bin/bash

.PHONY: translate

translate:
	@echo "Running translation job (creates timestamped output dir)..."
	@./scripts/run_translation.sh
