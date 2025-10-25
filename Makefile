# Default file path
FILE_PATH_CREDENTIALS ?= libnet-d76db-firebase-adminsdk-ju1ex-d1382d36b4.json
FILE_PATH_CONFIG ?= firestoreConfig.json


fly_secrets:
	@echo "Setting secrets from file..."
	flyctl secrets set FIREBASE_CREDENTIALS=$(cat $(FILE_PATH_CREDENTIALS) | base64)
	flyctl secrets set FIRESTORE_CONFIG=$(cat $(FILE_PATH_CONFIG) | base64)
	@if [ -f ./.env ]; then \
		. ./.env; \
		[ -n "$UPSTASH_REDIS_PASS" ] && flyctl secrets set UPSTASH_REDIS_PASS=$$UPSTASH_REDIS_PASS; \
		[ -n "$UPSTASH_REDIS_USER" ] && flyctl secrets set UPSTASH_REDIS_USER=$$UPSTASH_REDIS_USER; \
		[ -n "$UPSTASH_REDIS_REST_TOKEN" ] && flyctl secrets set UPSTASH_REDIS_REST_TOKEN=$$UPSTASH_REDIS_REST_TOKEN; \
		[ -n "$OPENAI_API_KEY" ] && flyctl secrets set OPENAI_API_KEY=$$OPENAI_API_KEY; \
		[ -n "$CELERY_BROKER_URL" ] && flyctl secrets set CELERY_BROKER_URL=$$CELERY_BROKER_URL; \
		[ -n "$REDIS_REST_URL" ] && flyctl secrets set REDIS_REST_URL=$$REDIS_REST_URL; \
		[ -n "$REDIS_REST_PASS" ] && flyctl secrets set REDIS_REST_PASS=$$REDIS_REST_PASS; \
		[ -n "$AWS_ACCESS_KEY_ID" ] && flyctl secrets set AWS_ACCESS_KEY_ID=$$AWS_ACCESS_KEY_ID; \
		[ -n "$AWS_SECRET_ACCESS_KEY" ] && flyctl secrets set AWS_SECRET_ACCESS_KEY=$$AWS_SECRET_ACCESS_KEY; \
 	fi
	@echo "Secrets set successfully!"

build:
	@echo "Building the project..."

run:
	@echo "Running the project..."

clean:
	@echo "Cleaning the project..."