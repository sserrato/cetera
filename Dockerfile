FROM socrata/java8

ENV CETERA_ROOT /srv/cetera/
ENV CETERA_ARTIFACT cetera-assembly.jar
ENV CETERA_CONFIG /etc/cetera.conf
ENV USE_CUSTOM_RANKER false

RUN mkdir -p $CETERA_ROOT

COPY $CETERA_ARTIFACT $CETERA_ROOT
COPY cetera.conf.j2 /etc/
COPY ship.d /etc/ship.d

EXPOSE 5704
