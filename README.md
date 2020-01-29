------------------
TODO
- demonstrate in integration test the exactly-once message delivery 
    * transactionality (https://www.baeldung.com/kafka-exactly-once)
    * https://kafka.apache.org/24/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
    * https://www.confluent.io/blog/transactions-apache-kafka/
    etc
- demonstrate production code + docker compose for broker used in integration tests
- do same project with activemq
------------------

Please check out the integration tests

You can notice some errors in the logs: 

java.nio.file.NoSuchFileException: /var/folders/_v/5x5n23xj5m7848rx18frq5cc0000gn/T/kafka-1388449629311401191/.kafka_cleanshutdown
         at sun.nio.fs.UnixException.translateToIOException(UnixException.java:86) ~[na:1.8.0_222]
         at sun.nio.fs.UnixException.rethrowAsIOException(UnixException.java:102) ~[na:1.8.0_222]
         at sun.nio.fs.UnixException.rethrowAsIOException(UnixException.java:107) ~[na:1.8.0_222]
         at sun.nio.fs.UnixFileSystemProvider.newByteChannel(UnixFileSystemProvider.java:214) ~[na:1.8.0_222]




Please read this thread: 
https://stackoverflow.com/questions/55293712/embeddedkafka-shutdown-errors
