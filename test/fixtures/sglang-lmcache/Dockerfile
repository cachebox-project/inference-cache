# SPDX-FileCopyrightText: 2026 The inference-cache Authors
#
# SPDX-License-Identifier: Apache-2.0

# Phase-3 connector-ready SGLang validation fixture. This image belongs to the
# inference owner; CacheBackend never injects or replaces it. Build for
# linux/amd64, push to the GPU test cluster's registry, and record the resulting
# derived digest in the migration roadmap before running the GPU matrix.
FROM lmsysorg/sglang@sha256:1c64fde976bdf0d56474a30bccbcfc19667e5b3ab34c826a534c9d6aaca41212

RUN python3 -m pip install --no-cache-dir "lmcache==0.5.3" \
    && python3 -c "import importlib.metadata as m; from lmcache.integration.sglang.multi_process_adapter import LMCacheMPConnector; assert m.version('lmcache') == '0.5.3'"

LABEL org.opencontainers.image.title="inference-cache SGLang LMCache connector fixture" \
      org.opencontainers.image.description="SGLang v0.5.13.post1-cu130 with LMCache 0.5.3 for PodLocal MP validation" \
      io.inferencecache.lmcache-connector-profile="sglang-lmcache-mp-v1" \
      io.inferencecache.lmcache-client-version="0.5.3"
