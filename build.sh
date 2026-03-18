#!/bin/bash
readonly IMAGE_TAG=${1:-0.1.0a5}
readonly IMAGE=nachocode/mictlanx:router-${IMAGE_TAG}
readonly PUSH_FLAG=${2:-1}
readonly NO_CACHE_FLAG=${3:-0}


if [ "$NO_CACHE_FLAG" -eq 1 ]; then
	echo "Building without cache"
	docker build --no-cache -f ./Dockerfile -t $IMAGE .
	exit 0
else
	docker build -f ./Dockerfile -t $IMAGE .
fi

if [ "$PUSH_FLAG" -eq 1 ]; then
	echo "Pushing image: $IMAGE"
	docker push $IMAGE
else
	echo "Skipping push"
fi 
