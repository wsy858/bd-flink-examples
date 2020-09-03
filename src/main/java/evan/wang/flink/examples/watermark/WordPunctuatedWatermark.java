package evan.wang.flink.examples.watermark;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * 根据特定条件，间断性的生成watermark
 */
public class WordPunctuatedWatermark implements AssignerWithPunctuatedWatermarks<Word> {

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(Word lastElement, long extractedTimestamp) {
        System.out.println(extractedTimestamp % 3 == 0 ? new Watermark(extractedTimestamp) : null);
        return extractedTimestamp % 3 == 0 ? new Watermark(extractedTimestamp) : null;
    }

    @Override
    public long extractTimestamp(Word element, long previousElementTimestamp) {
        return element.getTimestamp();
    }
}
