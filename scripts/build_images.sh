#!/bin/sh

GIT_COMMIT_SHA=$(git -C "$(git rev-parse --show-superproject-working-tree --show-toplevel | head -1)" rev-parse HEAD || true)
if [ -z "$GIT_COMMIT_SHA" ]; then
  GIT_COMMIT_SHA="[unknown]"
fi

print_help () {
cat <<-EOF
  Usage: $0 [OPTION]...

  Builds the proxy-whitelist Docker images.
  OPTIONS:
    --push                 Push built images to registry
    --tag=TAG_NAME         Name of tag used for the image
    --help,-h,-?           Display this help screen and exit
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --tag=*)
        TAG="${1#*=}"
        ;;
    --push)
        PUSH=1
        ;;
    --help|-h|-\?)
        print_help
        exit 0
        ;;
    -*)
        echo "ERROR: '$1' is not a valid option"
        exit 1
        ;;
    *)
        echo "ERROR: '$1' is not a valid positional argument"
    esac
    shift
done

TAG=${TAG:-$(echo "$GIT_COMMIT_SHA" | cut -c1-8)}

if [ -z "$TAG" ]; then
  echo "No tag, please pass it at first argument" >&2
  exit 1
fi

set -eu pipefail

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

export DOCKER_BUILDKIT=1

REGISTRY="registry.gitlab.syncad.com/hive/proxy-whitelist"

# Try to pull latest images for cache
echo "Pulling latest images for cache..."
docker pull "${REGISTRY}:develop" 2>/dev/null || true
docker pull "${REGISTRY}/postgrest-rewriter:develop" 2>/dev/null || true

# Build main proxy-whitelist image
echo "Building proxy-whitelist..."
docker build \
  --cache-from "${REGISTRY}:develop" \
  --build-arg BUILDKIT_INLINE_CACHE=1 \
  -t "${REGISTRY}:${TAG}" \
  "${SCRIPTPATH}/.."

echo "Built proxy-whitelist image tag ${TAG}"

# Build PostgREST rewriter image
echo "Building postgrest-rewriter..."
docker build \
  --cache-from "${REGISTRY}/postgrest-rewriter:develop" \
  --build-arg BUILDKIT_INLINE_CACHE=1 \
  -f "${SCRIPTPATH}/../Dockerfile.rewriter" \
  -t "${REGISTRY}/postgrest-rewriter:${TAG}" \
  "${SCRIPTPATH}/.."

echo "Built postgrest-rewriter image tag ${TAG}"

if [ -n "${PUSH:-}" ]; then
  docker push "${REGISTRY}:${TAG}"
  docker push "${REGISTRY}/postgrest-rewriter:${TAG}"
  echo "Pushed image tags ${TAG}"

  # Add 'latest' tags on develop branch
  if [ "${CI_COMMIT_BRANCH:-}" = "${CI_DEFAULT_BRANCH:-develop}" ]; then
    echo "Tagging with 'latest' for develop branch..."
    docker tag "${REGISTRY}:${TAG}" "${REGISTRY}:latest"
    docker push "${REGISTRY}:latest"
    docker tag "${REGISTRY}/postgrest-rewriter:${TAG}" "${REGISTRY}/postgrest-rewriter:latest"
    docker push "${REGISTRY}/postgrest-rewriter:latest"
    echo "Pushed 'latest' tags"
  fi

  # Add version tags on protected tags
  if [ -n "${CI_COMMIT_TAG:-}" ] && [ "${CI_COMMIT_REF_PROTECTED:-}" = "true" ]; then
    echo "Tagging with version tag '${CI_COMMIT_TAG}'..."
    docker tag "${REGISTRY}:${TAG}" "${REGISTRY}:${CI_COMMIT_TAG}"
    docker push "${REGISTRY}:${CI_COMMIT_TAG}"
    docker tag "${REGISTRY}/postgrest-rewriter:${TAG}" "${REGISTRY}/postgrest-rewriter:${CI_COMMIT_TAG}"
    docker push "${REGISTRY}/postgrest-rewriter:${CI_COMMIT_TAG}"
    echo "Pushed version tags '${CI_COMMIT_TAG}'"
  fi
fi
