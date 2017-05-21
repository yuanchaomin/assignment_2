import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import static java.lang.Math.sqrt;

/**
 * Created by Chaomin on 5/21/2017.
 */
public class Reclustering {
    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //args format:(input_label_points_file_dir, input_last_centerid_file_dir, output_task3_result_dir)
        if (args.length >= 3) {
            String input_label_points_file_dir = args[0];
            String input_last_centerid_file_dir = args[1];
            String output_task3_result_dir = args[2];

            DataSet<Tuple4<Integer, Double, Double, Double>> label_points = env.readCsvFile(input_label_points_file_dir)
                    .includeFields("1111")
                    .types(Integer.class, Double.class, Double.class, Double.class);



            DataSet<Tuple4<Integer, Double, Double, Double>> center_points = env.readCsvFile(input_last_centerid_file_dir)
                    .includeFields("1111")
                    .types(Integer.class, Double.class, Double.class, Double.class);

            DataSet<Tuple7<Integer, Double, Double, Double, Double, Double, Double>> joined_points_and_centerid = label_points
                    .join(center_points)
                    .where(0)
                    .equalTo(0)
                    .projectFirst(0,1,2,3)
                    .projectSecond(1,2,3);

            DataSet<Tuple5<Integer, Double, Double, Double, Double>> error_of_points = joined_points_and_centerid
                    .flatMap((tuple, out) -> {
                        int cluster_id = tuple.f0;
                        double ly6c = tuple.f1;
                        double cd11b = tuple.f2;
                        double sca1 = tuple.f3;
                        double error = sqrt((tuple.f1 - tuple.f5)*(tuple.f1 - tuple.f5) + (tuple.f2 - tuple.f5)*(tuple.f2 - tuple.f5)
                                        + (tuple.f3 - tuple.f6)*(tuple.f3 - tuple.f6));

                        out.collect( new Tuple5<>(cluster_id, ly6c, cd11b, sca1, error));

                    })
                    .returns(new TupleTypeInfo(TypeInformation.of(Integer.class), TypeInformation.of(Double.class), TypeInformation.of(Double.class),
                            TypeInformation.of(Double.class), TypeInformation.of(Double.class)));


            error_of_points.print();
        }


    }
}

