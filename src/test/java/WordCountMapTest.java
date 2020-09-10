import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;


public class WordCountMapTest {

    private Mapper<Object, Text, Text, VIntWritable> mapper;
    private MapDriver<Object, Text, Text, VIntWritable> driver;

    @Before
    public void setUp() {
        mapper = new WordCountMapper();
        driver = new MapDriver<>(mapper);
    }

    /**
     * {@code MapDriver.runTest(false)}: the order does not matter.
     *
     * @throws IOException if io exception occurs
     */
    @Test
    public void testWordCountMapper() throws IOException {
        driver.withInput(new Text(""), new Text("bar hey whatever"))
                .withOutput(new Text("bar"), new VIntWritable(1))
                .withOutput(new Text("whatever"), new VIntWritable(1))
                .withOutput(new Text("hey"), new VIntWritable(1))
                .runTest(false);
    }
}
