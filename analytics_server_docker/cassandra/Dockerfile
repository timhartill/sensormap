FROM cassandra:3.11.2

WORKDIR /home/cassandra

#COPY ./config/entrypoint-wrap.sh .

#COPY ./config/schema.cql .

ENTRYPOINT ["/home/cassandra/entrypoint-wrap.sh"]

CMD ["cassandra", "-f"]
