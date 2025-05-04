package org.tut.fraudk1detect

import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.entity.Transaction
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class FraudDetector : KeyedProcessFunction<Long, Transaction, Alert>() {
    private
    val LOG: Logger = LoggerFactory.getLogger(FraudDetector::class.java)

    val serialVersionUID: Long = 1L

    val SMALL_AMOUNT: Double = 1.00

    val LARGE_AMOUNT: Double = 500.00

    val ONE_MINUTE: Long = (60 * 1000).toLong()

    @Transient
    private var flagState: ValueState<Boolean>? = null

    @Transient
    private var timerState: ValueState<Long>? = null

    override fun open(openContext: OpenContext?) {
        val flagDescriptor: ValueStateDescriptor<Boolean> = ValueStateDescriptor("flag", Types.BOOLEAN)
        flagState = getRuntimeContext().getState(flagDescriptor)

        val timerDescriptor: ValueStateDescriptor<Long> = ValueStateDescriptor("time-state", Types.LONG)
        timerState = getRuntimeContext().getState(timerDescriptor)
    }

    @Throws(Exception::class)
    override fun processElement(
        transaction: Transaction,
        context: Context,
        collector: Collector<Alert>
    ) {
        val lastTransactionWasSmall: Boolean? = flagState?.value()
        LOG.warn("Transaction: " + transaction.toString())

        if (lastTransactionWasSmall != null) {
            if (transaction.getAmount() > LARGE_AMOUNT) {
                val alert: Alert = Alert()
                alert.setId(transaction.getAccountId())

                collector.collect(alert)
            }

            cleanUp(context)
        }

        if (transaction.getAmount() < SMALL_AMOUNT) {
            flagState?.update(true)
            val timer: Long = context.timerService().currentProcessingTime() + ONE_MINUTE
            context.timerService().registerProcessingTimeTimer(timer)
            timerState?.update(timer)
        }
    }


    override fun onTimer(timestamp: Long, ctx: OnTimerContext?, out: Collector<Alert?>?) {
        // remove flag after 1 minute
        timerState?.clear()
        flagState?.clear()
    }

    @Throws(Exception::class)
    private fun cleanUp(ctx: Context) {
        // delete timer
        val timer: Long = timerState!!.value()
        ctx.timerService().deleteProcessingTimeTimer(timer)

        // clean up all state
        timerState?.clear()
        flagState?.clear()
    }
}