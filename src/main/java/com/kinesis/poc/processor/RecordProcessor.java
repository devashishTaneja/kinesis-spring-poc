package com.kinesis.poc.processor;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class RecordProcessor implements IRecordProcessor {

    @Override
    public void initialize(InitializationInput initializationInput) {

    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        processRecordsInput.getRecords()
                .forEach(record -> System.out.println(new String(record.getData().array())));
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {

    }
}
