package evan.wang.flink.examples.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * 根据特定条件，标记生成watermark
 */
public class WordPunctuatedWatermarkNew implements WatermarkGenerator<Word> {

    @Override
    public void onEvent(Word word, long eventTimestamp, WatermarkOutput watermarkOutput) {
        if(eventTimestamp % 3 ==0){
            watermarkOutput.emitWatermark(new Watermark(eventTimestamp));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        // onEvent 中已经实现
    }
}
