package evan.wang.flink.examples.watermark;

import evan.wang.flink.examples.utils.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

import static evan.wang.flink.examples.utils.DateUtil.YYYY_MM_DD_HH_MM_SS;

/**
 * 实现类似于系统自带的 {@link BoundedOutOfOrdernessWatermarks}
 */
@Slf4j
public class WordPeriodicWatermarkNew implements WatermarkGenerator<Word> {

    private final long maxTimeLag  = 5000; // 5秒

    private long currentMaxTimestamp;

    @Override
    public void onEvent(Word event, long eventTimestamp, WatermarkOutput output) {
        currentMaxTimestamp = Math.max(currentMaxTimestamp, event.getTimestamp());
        log.info("eventTimestamp = {}, {},  waterMaker = {}", currentMaxTimestamp,
                DateUtil.format(currentMaxTimestamp, YYYY_MM_DD_HH_MM_SS),
                (currentMaxTimestamp - maxTimeLag));
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        long watermark = currentMaxTimestamp - maxTimeLag;
        // 发出的 watermark = 当前最大时间戳 - 最大乱序时间
        output.emitWatermark(new Watermark(watermark));
        log.info("CurrentWatermark = {}, {}", watermark, DateUtil.format(watermark, YYYY_MM_DD_HH_MM_SS));
    }
}
