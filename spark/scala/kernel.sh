#!/usr/bin/env bash
set -euo pipefail

SCALA_OUTPUT_DIR=$(mktemp -d)
export SCALA_OUTPUT_DIR
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:+$JAVA_TOOL_OPTIONS }--add-opens=java.base/java.nio=ALL-UNNAMED"
trap 'rm -rf "$SCALA_OUTPUT_DIR"' EXIT
spark_classpath=$(find "$SPARK_HOME/jars" -name '*.jar' \
  ! -name 'scala-library-*' ! -name 'scala-reflect-*' \
  ! -name 'scala-compiler-*' ! -name 'spark-repl_*' -printf '%p:')

/opt/scala-kernel --extra-class-path "$spark_classpath" \
  --output-directory "$SCALA_OUTPUT_DIR" "$@"
