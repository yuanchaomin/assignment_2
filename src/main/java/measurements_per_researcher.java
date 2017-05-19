/**
 * Created by Chaomin on 5/19/2017.
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
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

            DataSet<Tuple3<String, Integer, Integer>> filtered_record = env
                    .readCsvFile(measurement_file_input_dir)
                    .includeFields("11100000000000000")
                    .types(String.class, Integer.class, Integer.class)
                    .flatMap((line, out) -> {
                        String lineString = line.toString();
                        String[] values = lineString.split(",");

                        String sample = values[0];
                        String fsc_a = values[1];
                        String ssc_a = values[2];

                        for(String value : values) {
                            out.collect(new Tuple3<String, Integer, Integer>(sample, Integer.parseInt(fsc_a),Integer.parseInt(ssc_a)));
                        }
                    });

            filtered_record.writeAsCsv(output_file_dir);
            env.execute();
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