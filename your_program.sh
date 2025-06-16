#!/bin/sh

set -e 

(
  cd "$(dirname "$0")" # Ensure compile steps are run within the repository directory
  sbt assembly
)

exec java -jar ./target/scala-3.7.1/redis.jar "$@"
