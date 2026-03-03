#!/bin/sh

GIT_COMMIT_SHA=$(git -C "$(git rev-parse --show-superproject-working-tree --show-toplevel | head -1)" rev-parse HEAD || true)
if [ -z "$GIT_COMMIT_SHA" ]; then
  GIT_COMMIT_SHA="[unknown]"
fi

print_help () {
cat <<-EOF
  Usage: $0 [OPTION]...

  Builds the proxy-whitelist Docker image.
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

# Try to pull latest image for cache
echo "Pulling latest image for cache..."
docker pull "registry.gitlab.syncad.com/hive/proxy-whitelist:develop" 2>/dev/null || true

echo "Building proxy-whitelist..."
docker build \
  --cache-from "registry.gitlab.syncad.com/hive/proxy-whitelist:develop" \
  --build-arg BUILDKIT_INLINE_CACHE=1 \
  -t "registry.gitlab.syncad.com/hive/proxy-whitelist:${TAG}" \
  "${SCRIPTPATH}/.."

echo "Built image tag ${TAG}"

if [ -n "${PUSH:-}" ]; then
  docker push "registry.gitlab.syncad.com/hive/proxy-whitelist:${TAG}"
  echo "Pushed image tag ${TAG}"

  # Add 'latest' tags on develop branch
  if [ "${CI_COMMIT_BRANCH:-}" = "${CI_DEFAULT_BRANCH:-develop}" ]; then
    echo "Tagging with 'latest' for develop branch..."
    docker tag "registry.gitlab.syncad.com/hive/proxy-whitelist:${TAG}" "registry.gitlab.syncad.com/hive/proxy-whitelist:latest"
    docker push "registry.gitlab.syncad.com/hive/proxy-whitelist:latest"
    echo "Pushed 'latest' tag"
  fi

  # Add version tags on protected tags
  if [ -n "${CI_COMMIT_TAG:-}" ] && [ "${CI_COMMIT_REF_PROTECTED:-}" = "true" ]; then
    echo "Tagging with version tag '${CI_COMMIT_TAG}'..."
    docker tag "registry.gitlab.syncad.com/hive/proxy-whitelist:${TAG}" "registry.gitlab.syncad.com/hive/proxy-whitelist:${CI_COMMIT_TAG}"
    docker push "registry.gitlab.syncad.com/hive/proxy-whitelist:${CI_COMMIT_TAG}"
    echo "Pushed version tag '${CI_COMMIT_TAG}'"
  fi
fi
