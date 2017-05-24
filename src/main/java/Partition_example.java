/**
 * Created by Chaomin on 5/25/2017.
 */
import org.apache.avro.Schema;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.operators.Order;

public class Partition_example {
    public static class MyPartitioner implements Partitioner<Integer> {
        @Override
        public int partition(Integer key, int numPartitions) {
            return key % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, String>> data = env.fromElements(
                new Tuple2<Integer, String>(1, "a"), new Tuple2<Integer, String>(1, "b"),
                new Tuple2<Integer, String>(3, "c"), new Tuple2<Integer, String>(1, "d"),
                new Tuple2<Integer, String>(5, "e"), new Tuple2<Integer, String>(6, "f"));

        DataSet<Tuple2<Integer, String>> partitionedData = data
                .partitionByRange(1).withOrders(Order.DESCENDING);
        //data.print();
        partitionedData.print();
    }
}