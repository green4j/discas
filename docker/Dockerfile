# The image for all three discas commands. The build context is staged by the `dockerContext`
# Gradle task and holds only this file, the entrypoint and the bundled jar.
#
# The base carries the JDK, not the JRE: jcmd, jstack, jmap and jinfo live in JDK-only modules, and
# a node's event loop is also its timer thread, which makes a thread dump the instrument to reach
# for. It is pinned to the JDK patch and the Alpine minor, so one discas version is one image.
FROM eclipse-temurin:25.0.4_7-jdk-alpine-3.21

ARG REVISION=unknown

LABEL org.opencontainers.image.source="https://github.com/green4j/discas" \
      org.opencontainers.image.revision="${REVISION}"

RUN addgroup -S -g 1000 discas \
 && adduser -S -u 1000 -G discas -H -D discas \
 && mkdir -p /var/lib/discas \
 && chown discas:discas /var/lib/discas

COPY discas-all.jar /opt/discas/lib/discas-all.jar
COPY bin/discas     /opt/discas/bin/discas

USER discas:discas

ENTRYPOINT ["/opt/discas/bin/discas"]
CMD ["node"]
