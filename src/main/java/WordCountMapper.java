import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper Utility for word count.
 */
public class WordCountMapper extends Mapper<Object, Text, Text, VIntWritable> {
    private static final VIntWritable one = new VIntWritable(1);
    private Text word = new Text();

    /**
     * Mapper for word count example.
     *
     * @param key input key of mapper
     * @param value input value of mapper
     * @param context output key/value pair of mapper
     * @throws IOException if io exception occurs
     * @throws InterruptedException if interrupted exception occurs
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}