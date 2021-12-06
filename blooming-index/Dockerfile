FROM library/cassandra:${cassandra-version}
COPY ${project.basedir}/${project.build.directory}/${project.build.finalName}.jar /opt/cassandra/lib/blooming-index.jar
COPY ${project.basedir}/${project.build.directory}/libs/* /opt/cassandra/lib/
EXPOSE 9042