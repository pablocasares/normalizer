#!/usr/bin/env bash
envsubst < /opt/normalizer/config/config_env.json > /opt/normalizer/config/config.json
envsubst < /opt/normalizer/config/log4j2_env.xml > /opt/normalizer/config/log4j2.xml

exec /opt/normalizer/bin/normalizer-start.sh /opt/normalizer/config/config.json
