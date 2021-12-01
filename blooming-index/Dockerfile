FROM library/cassandra:${cassandra-version}
COPY ${project.basedir}/${project.build.directory}/${project.build.finalName}.jar /opt/cassandra/lib/blooming-index.jar
EXPOSE 9042