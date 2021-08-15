package evan.wang.flink.examples.watermark;

import evan.wang.flink.examples.utils.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import static evan.wang.flink.examples.utils.DateUtil.YYYY_MM_DD_HH_MM_SS;

/**
 * 周期性的生成watermark
 * 生成器可以覆盖的场景是：数据源在一定程度上乱序, 或有一定的延迟。
 * */
@Slf4j
@Deprecated
public class WordPeriodicWatermark implements AssignerWithPeriodicWatermarks<Word> {

    private long currentTimestamp = Long.MIN_VALUE;

    @Override
    public long extractTimestamp(Word word, long previousElementTimestamp) {
        long eventTimestamp = word.getTimestamp();
        currentTimestamp = Math.max(eventTimestamp, currentTimestamp);
        log.info(
                "eventTimestamp = {}, {}, CurrentWatermark = {}, {}",
                eventTimestamp,
                DateUtil.format(eventTimestamp, YYYY_MM_DD_HH_MM_SS),
                getCurrentWatermark().getTimestamp(),
                DateUtil.format(getCurrentWatermark().getTimestamp(), YYYY_MM_DD_HH_MM_SS));
        return currentTimestamp;
    }

    @Override
    public Watermark getCurrentWatermark() {
        long maxTimeLag = 5000;
        return new Watermark(
                currentTimestamp == Long.MIN_VALUE
                        ? Long.MIN_VALUE
                        : currentTimestamp - maxTimeLag);
    }
}


