/**
 * Created by Chaomin on 5/19/2017.
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;


public class measurements_per_researcher {

    public static void main(String[] args) throws Exception {
        // where args[0] is input of measurement_file, and args[1] is the input of experiments table,
        // and the args[2] is the output file address.
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // get input file from local system
        if (args.length >= 3) {
            String measurement_file_input_dir = args[0];
            String experiment_file_input_dir = args[1];
            String output_file_dir = args[2];

            DataSet<Tuple2<String, String>> experiment_record = env
                    .readCsvFile(experiment_file_input_dir)
                    .ignoreFirstLine()
                    .includeFields("10000001")
                    .types(String.class, String.class)
                    .flatMap((line, out) -> {
                        String[] researchers = line.f1.trim().split("\\;\\s|\\;");
                        String sample = line.f0;

                        for (String rearcher : researchers){
                            out.collect(new Tuple2<String, String>(rearcher, sample));
                        }
                    })
                    .returns(new TupleTypeInfo(TypeInformation.of(String.class), TypeInformation.of(String.class)));

            DataSet<Tuple2<String, Integer>> filtered_record = env
                    .readCsvFile(measurement_file_input_dir)
                    .ignoreFirstLine()
                    .includeFields("11100000000000000")
                    .types(String.class, Integer.class, Integer.class)
                    .flatMap((line, out) -> {
                        String sample = line.f0;
                        int fsc_a = line.f1;
                        int ssc_a =  line.f2;

                        Boolean is_fsc_a_valid = ((fsc_a >= 1) && (fsc_a <= 150000));
                        Boolean is_ssc_a_valid = ((ssc_a >= 1) && (ssc_a <= 150000));

                        if ((is_fsc_a_valid ) && (is_ssc_a_valid)) {
                            out.collect(new Tuple2<> (sample, fsc_a));
                        }
                    })
                    .returns(new TupleTypeInfo(TypeInformation.of(String.class), TypeInformation.of(Integer.class)));

            DataSet<Tuple2<String, Integer>> one = filtered_record
                    .map(tuple -> new Tuple2<String, Integer>(tuple.f0, tuple.f1))
                    .returns(new TupleTypeInfo(TypeInformation.of(String.class), TypeInformation.of(Integer.class)));


            DataSet<Tuple2<String, Integer>> grouped_record = one
                    .groupBy(0)
                    .sortGroup(0, Order.ASCENDING)
                    .reduceGroup((tuples, out) -> {
                        String sample = "";
                        Integer count = 0;

                        for(Tuple2<String, Integer> tuple : tuples) {
                            sample = tuple.f0;
                            count += 1;
                        }

                        out.collect(new Tuple2<>(sample,count));
                    })
                    .returns(new TupleTypeInfo(TypeInformation.of(String.class), TypeInformation.of(Integer.class)));

            DataSet<Tuple2<Integer,String>> joined_record = grouped_record
                    .join(experiment_record)
                    .where(0)
                    .equalTo(1)
                    .projectFirst(1)
                    .projectSecond(0);


            DataSet<Tuple2<String, Integer>> sumed_record = joined_record
                    .groupBy(1)
                    .sortGroup(0, Order.DESCENDING)
                    .reduceGroup((tuples, out) -> {
                        String researcher = "";
                        Integer num_of_experiment = 0;

                        for(Tuple2<Integer,String> tuple : tuples) {
                            researcher = tuple.f1;
                            num_of_experiment += tuple.f0;
                        }
                                out.collect(new Tuple2<>(researcher, num_of_experiment));
                            }
                    )
                    .returns(new TupleTypeInfo(TypeInformation.of(String.class),TypeInformation.of(Integer.class)));



            sumed_record .writeAsCsv(output_file_dir,"\n", ",",  FileSystem.WriteMode.OVERWRITE).setParallelism(1);
            env.execute();
            //sumed_record .print();
            System.out.println("End of the program!");
        }
        else{
            System.out.println("Wrong input parameters!");
            System.out.println("End of the program!");
            System.exit(0);
        }
    }

    public static class Filter implements FlatMapFunction<String, Tuple3<String, Double, Double>> {
        @Override
        public void flatMap(String line, Collector<Tuple3<String, Double, Double>> out) {

                String [] column_values = line.split(",");
                String sample = column_values[0];
                String fsc_a = column_values[1];
                String ssc_a = column_values[2];

                Boolean is_fsc_a_valid = ((Integer.parseInt(fsc_a) >= 1) && (Integer.parseInt(fsc_a)<= 150000));
                Boolean is_ssc_a_valid = ((Integer.parseInt(ssc_a) >= 1) && (Integer.parseInt(ssc_a)<= 150000));

                if ((is_fsc_a_valid ) && (is_ssc_a_valid)) {
                            for(String column_value : column_values) {
                                out.collect(new Tuple3<String, Double, Double>(sample, Double.parseDouble(fsc_a),Double.parseDouble(ssc_a)));
                            }
                        }
        }

        }
}