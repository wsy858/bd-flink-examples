package evan.wang.flink.examples.watermark;

import evan.wang.flink.examples.utils.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

import static evan.wang.flink.examples.utils.DateUtil.YYYY_MM_DD_HH_MM_SS;


/**
 * 周期性的生成watermark
 */
@Slf4j
public class WordPeriodicWatermark implements AssignerWithPeriodicWatermarks<Word> {

    private long currentTimestamp = Long.MIN_VALUE;

    @Override
    public long extractTimestamp(Word word, long previousElementTimestamp) {
        long timestamp = word.getTimestamp();
        currentTimestamp = Math.max(timestamp, currentTimestamp);
        log.info("event timestamp = {}, {}, CurrentWatermark = {}, {}", word.getTimestamp(),
                DateUtil.format(word.getTimestamp(), YYYY_MM_DD_HH_MM_SS),
                getCurrentWatermark().getTimestamp(),
                DateUtil.format(getCurrentWatermark().getTimestamp(), YYYY_MM_DD_HH_MM_SS));
        return word.getTimestamp();
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        long maxTimeLag = 5000;
        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag);
    }
}
