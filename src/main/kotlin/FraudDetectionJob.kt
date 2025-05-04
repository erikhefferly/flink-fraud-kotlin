package org.tut.fraudk1detect

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.entity.Transaction
import org.apache.flink.walkthrough.common.sink.AlertSink
import org.apache.flink.walkthrough.common.source.TransactionSource

class FraudDetectionJob {

    fun execute() {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()

        val transactions: DataStream<Transaction> = env
            .addSource(TransactionSource())
            .name("transactions")

        val alerts: DataStream<Alert> = transactions
            .keyBy(Transaction::getAccountId)
            .process(FraudDetector())
            .name("fraud-detector")

        alerts
            .addSink(AlertSink())
            .name("send-alerts")

        env.execute("Fraud Detection")
    }
}
