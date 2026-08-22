.PHONY: install check build test

install:
	@cd sdks/typescript && npm ci

check:
	@cd sdks/typescript && npm run typecheck

build:
	@cd sdks/typescript && npm run build

test:
	@cd sdks/typescript && npm test
