package com.pravin.simple.file.read;

import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SimpleFileReadApplication {
  public static void main(String[] args) throws Exception {
    //SpringApplication.run(SimpleFileReadApplication.class, args);
    StreamExecutionEnvironment env
        = StreamExecutionEnvironment.getExecutionEnvironment();

    // monitor directory, checking for new files
    TextInputFormat format = new TextInputFormat(
        new org.apache.flink.core.fs.Path("file:///tmp/flink-tmp/"));

    FilePathFilter.createDefaultFilter();
    DataStream<String> inputStream = env.readFile(
        format,
        "file:///tmp/flink-tmp",
        FileProcessingMode.PROCESS_CONTINUOUSLY,
        100);

    inputStream.print();

    // execute program
    env.execute("Read_File_From_Dir");
  }
}



