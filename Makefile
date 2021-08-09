GIT_REF := $(shell git describe --always)
BRANCH_NAME ?= master

.PHONY: deploy
deploy:
	@gcloud --project spectra-tokyo \
	  builds submit \
	  --config cloudbuild.yaml \
	  --substitutions=BRANCH_NAME=${BRANCH_NAME},COMMIT_SHA=${GIT_REF}
