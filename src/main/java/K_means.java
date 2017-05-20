import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

/**
 * Created by Chaomin on 5/20/2017.
 */
public class K_means {

    public  static void main(String[] args) throws Exception {
        // where args[0] is input of measurement_file, and args[1] is the input of experiments table,
        // and the args[2] is the output file address.
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // get input file from local system
        if (args.length >= 2) {
            String measurement_file_input_dir = args[0];
            String output_file_dir = args[1];

            DataSet<Tuple6<String, Integer,Integer,Double,Double,Double>> filtered_record = env
                    .readCsvFile(measurement_file_input_dir)
                    .ignoreFirstLine()
                    .includeFields("11101011000000000")
                    .types(String.class, Integer.class, Integer.class, Double.class, Double.class, Double.class)
                    .flatMap((line, out) -> {
                        String sample = line.f0;
                        Integer fsc_a = line.f1;
                        Integer ssc_a =  line.f2;
                        Double ly6g = line.f3;
                        Double sca1 = line.f4;
                        Double cd11b = line.f5;

                        Boolean is_fsc_a_valid = ((fsc_a >= 1) && (fsc_a <= 150000));
                        Boolean is_ssc_a_valid = ((ssc_a >= 1) && (ssc_a <= 150000));

                        if ((is_fsc_a_valid ) && (is_ssc_a_valid)) {
                            out.collect(new Tuple6<> (sample, fsc_a, ssc_a, ly6g, sca1,cd11b));
                        }
                    })
                    .returns(new TupleTypeInfo(TypeInformation.of(String.class), TypeInformation.of(Integer.class)));

            DataSet<Tuple4<String, Double, Double, Double>> for_kmeans_record = filtered_record
                    .map(tuple -> new Tuple4<String, Double, Double, Double>(tuple.f0, tuple.f3, tuple.f4, tuple.f5))
                    .returns(new TupleTypeInfo(TypeInformation.of(String.class), TypeInformation.of(Double.class), TypeInformation.of(Double.class), TypeInformation.of(Double.class)));
            //grouped_record.writeAsCsv(output_file_dir);
            //env.execute();
            for_kmeans_record.print();
            System.out.println("End of the program!");
        }
        else{
            System.out.println("Wrong input parameters!");
            System.out.println("End of the program!");
            System.exit(0);
        }
    }
}
