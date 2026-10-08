#!/usr/bin/env bash
set -euo pipefail

REPOSITORY_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MAVEN_REPOSITORY="${MAVEN_REPO_LOCAL:-${REPOSITORY_ROOT}/.cache/m2}"
STAGING_DIRECTORY="$(mktemp -d)"
trap 'rm -rf "${STAGING_DIRECTORY}"' EXIT

mvn \
  -f "${REPOSITORY_ROOT}/flink/pom.xml" \
  -Dmaven.repo.local="${MAVEN_REPOSITORY}" \
  -DskipTests \
  clean package

mkdir -p "${STAGING_DIRECTORY}/lib"
cp "${REPOSITORY_ROOT}/flink/job.py" "${STAGING_DIRECTORY}/main.py"
cp "${REPOSITORY_ROOT}/flink/detector_core.py" "${STAGING_DIRECTORY}/detector_core.py"
cp \
  "${REPOSITORY_ROOT}/flink/target/bagguard-pyflink-dependencies.jar" \
  "${STAGING_DIRECTORY}/lib/bagguard-pyflink-dependencies.jar"

mkdir -p "${REPOSITORY_ROOT}/flink/dist"
rm -f "${REPOSITORY_ROOT}/flink/dist/bagguard-risk-detector.zip"

# ZIP archives carry filesystem timestamps. Normalizing them keeps identical
# source builds byte-for-byte stable for the content-addressed S3 object key.
find "${STAGING_DIRECTORY}" -exec touch -t 202601010000 {} +
(
  cd "${STAGING_DIRECTORY}"
  zip -X -q -r \
    "${REPOSITORY_ROOT}/flink/dist/bagguard-risk-detector.zip" \
    main.py detector_core.py lib
)

unzip -t "${REPOSITORY_ROOT}/flink/dist/bagguard-risk-detector.zip"
