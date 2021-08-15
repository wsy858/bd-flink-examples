package evan.wang.flink.examples.watermark;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;


/**
 * 根据特定条件，标记生成watermark
 */
@Deprecated
public class WordPunctuatedWatermark implements AssignerWithPunctuatedWatermarks<Word> {

    @Override
    public Watermark checkAndGetNextWatermark(Word lastElement, long extractedTimestamp) {
        return extractedTimestamp % 3 == 0 ? new Watermark(extractedTimestamp) : null;
    }

    @Override
    public long extractTimestamp(Word element, long previousElementTimestamp) {
        return element.getTimestamp();
    }
}
