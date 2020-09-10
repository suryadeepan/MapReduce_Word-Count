import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, VIntWritable, Text, VIntWritable> {
    /**
     * Reduce function for word count example.
     * @param key input key of reducer
     * @param values input values of reduce which is iterable
     * @param context output key/value pair of reducer
     * @throws IOException if io exception occurs
     * @throws InterruptedException if interrupted exception occurs
     */
    @Override
    public void reduce(Text key, Iterable<VIntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (VIntWritable vintWritable : values) {
            count += vintWritable.get();
        }
        context.write(key, new VIntWritable(count));
    }
}