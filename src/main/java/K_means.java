import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.io.*;
import java.util.*;

/**
 * Created by Chaomin on 5/20/2017.
 */
public class K_means {

    public  static void main(String[] args) throws Exception {
        // where args[0] is input of measurement_file, and args[1] is the input of experiments table,
        // and the args[2] is the output file address.
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // get input file from local system

        // args format: (input_measurement_file_dir, output_task2_resuslt_dir,ouput_label_points_file_dir, output_last_centerid_file_dir, K(para for K_means algorithm), iteration_size)
        if (args.length >= 4) {
            String measurement_file_input_dir = args[0];
            String output_task2_resuslt_dir = args[1];
            String ouput_label_points_file_dir= args[3];
            String output_last_centerid_file_dir = args[4];

            //generate dataset points
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


            DataSet<Tuple3<Double, Double, Double>> for_kmeans_record = filtered_record
                    .map(tuple  -> new Tuple3<Double, Double, Double>(tuple.f3, tuple.f4, tuple.f5))
                    .returns(new TupleTypeInfo(TypeInformation.of(Double.class), TypeInformation.of(Double.class), TypeInformation.of(Double.class)));

            DataSet<Point> points = for_kmeans_record
                    .map(new collect());


            //generate dataset centroid_points
            int k = 5; //defulet k = 5
            int iterate_size = 10; //defult iteration = 10
            if(args.length>6){
                k= Integer.parseInt(args[5]);
            }
            if(args.length>7){
                iterate_size = Integer.parseInt(args[6]);
            }
            List<Center> genCenter = new ArrayList<Center>();

            for(int i =0;i<k;i++){
                double ly6c = Math.random()*5;
                double CD11b = Math.random()*5;
                double SCA1 = Math.random()*5;
                int c = i+1;
                //System.out.println(ly6c+", "+CD11b+", "+SCA1);
                genCenter.add(new Center(ly6c,CD11b,SCA1,c));
                //System.out.println(genCenter.size());7
            }  //generate center point

            DataSet<Center> centroids = env.fromCollection(genCenter);
            //centroids.print();

            //set number of bulk iterations
            IterativeDataSet<Center> first_centroids = centroids.iterate(iterate_size);

            DataSet<Center> new_center_id = points
                    .map(new Select_Centroid()).withBroadcastSet(first_centroids, "centerids")
                    .map(new Count())
                    .groupBy(0).reduce(new Centroid_aggregate())
                    .map(new Cal_centroid());



            // put new_center_id into next iteration
            DataSet<Center> last_centroids = first_centroids.closeWith(new_center_id);

            DataSet<Tuple2<Integer, Point>> labeled_points = points
                    .map(new Select_Centroid()).withBroadcastSet(last_centroids, "centerids");

            DataSet<Tuple4<Integer, Double,Double,Double>> labeled_points_tuple = labeled_points
                    .map(tuple -> new Tuple4<Integer, Double, Double, Double>(tuple.f0, tuple.f1.ly6c, tuple.f1.CD11b, tuple.f1.SCA1))
                    .returns(new TupleTypeInfo(TypeInformation.of(Integer.class), TypeInformation.of(Double.class), TypeInformation.of(Double.class),
                            TypeInformation.of(Double.class)));


            // return (id, count), where count equal 1 for each record.
            DataSet<Tuple2<Integer, Integer>> append_count_column_record = labeled_points
                    .flatMap((tuple, out) -> {
                        int id = tuple.f0;
                        int count = 1;

                        out.collect(new Tuple2<>(id, count));
                    })
                    .returns(new TupleTypeInfo(TypeInformation.of(Integer.class), TypeInformation.of(Integer.class)));

            // calculate the number of points within one cluster, return(id, count) where count is the number in that cluster(id)
            DataSet<Tuple2<Integer,Integer>> sum_number_of_points = append_count_column_record
                    .groupBy(0)
                    .sum(1);



            DataSet<Tuple4<Integer, Double, Double, Double >> last_centerid_with_id = last_centroids
                    .flatMap((center_point, out) -> {
                        int cluster_id = center_point.clusterID;
                        Double ly6c = center_point.ly6c;
                        Double cd11b = center_point.CD11b;
                        Double sca1 = center_point.SCA1;

                        out.collect( new Tuple4<>(cluster_id, ly6c, cd11b, sca1 ));

                    })
                    .returns(new TupleTypeInfo(TypeInformation.of(Integer.class), TypeInformation.of(Double.class), TypeInformation.of(Double.class), TypeInformation.of(Double.class)));


            // return the final result with format(cluster_id, number_of_measurements, ly6c, cd11b, sca1)
            DataSet<Tuple5<Integer, Integer,Double,Double,Double>> final_result = sum_number_of_points
                    .join(last_centerid_with_id)
                    .where(0)
                    .equalTo(0)
                    .projectFirst(0,1)
                    .projectSecond(1,2,3);




            final_result.writeAsCsv(output_task2_resuslt_dir, "\n", ",",  FileSystem.WriteMode.OVERWRITE).setParallelism(1);
            labeled_points_tuple.writeAsCsv(ouput_label_points_file_dir, "\n", ",",  FileSystem.WriteMode.OVERWRITE).setParallelism(1);
            last_centerid_with_id.writeAsCsv(output_last_centerid_file_dir, "\n", ",",  FileSystem.WriteMode.OVERWRITE).setParallelism(1);
            env.execute();
            sort_file(args[1], args[2], "task2_result");
            //final_result .print();

            System.out.println("End of the program!");
        }
        else{
            System.out.println("Wrong input parameters!");
            System.out.println("End of the program!");
            System.exit(0);
        }
    }

    public static void sort_file(String input_address, String output_address, String s) throws FileNotFoundException, IOException{

        HashMap<String, String> a = new HashMap<>();
        //s = "task2_result"
        BufferedReader input = new BufferedReader(new FileReader(input_address));
        String line = input.readLine();

        while (line != null) {
            String columns [] = line.split(",");
            String rest = columns[1] + "\t" + columns[2] + "\t" +
                            columns[3] + "\t"  + columns[4];
            a.put(columns[0], rest);
            line = input.readLine();
        }

        ArrayList<Map.Entry<String,String>> List = new ArrayList<Map.Entry<String,String>>(a.entrySet());
        //Comparator<Integer> comparator = Collections.reverseOrder();
        Collections.sort(List, new Comparator<Map.Entry<String,String>>()
        {
            public int compare( Map.Entry<String,String> o1, Map.Entry<String,String> o2 ) {
                return (o1.getKey()).compareTo(o2.getKey());
            }
        } );

        new File(output_address.replace(s, "")).mkdirs();
        FileWriter writer = new FileWriter(output_address);

        for (Map.Entry<String,String> j : List) {
            writer.append(j.getKey())
                    .append("\t")
                    .append(j.getValue())
                    .append("\n");
        }


        writer.close();
        //System.out.println(List);
    }


    public static class collect implements MapFunction<Tuple3<Double, Double, Double>, Point> {

        public Point map(Tuple3<Double, Double, Double> t) throws Exception {
                Double [] list = new Double[3];
                list[0] = t.f0;
                list[1] = t.f1;
                list[2] = t.f2;

                return new Point(list[0],list[1],list[2]);
        }
    }

    public static class Select_Centroid extends RichMapFunction<Point, Tuple2<Integer, Point>> {
        private Collection<Center> centerids;
        //read the initial centerid from genCenter(a collection we create)

        public void open(Configuration parameters) {
            this.centerids = getRuntimeContext().getBroadcastVariable("centerids");
        }

        public Tuple2<Integer, Point> map(Point p) throws  Exception {

            double minDis = Double.MAX_VALUE;
            int closest_center_id =  -1;

            //calculate distance with all centerid in the centerid_dataset.
            for(Center centerid : centerids) {
                double dis = p.euclideanDistance(centerid);

                if (dis < minDis) {
                    minDis = dis;
                    closest_center_id = centerid.clusterID;
                }
            }

            //return the result
            return new Tuple2<>(closest_center_id,p);

        }
    }

    public static final class Count implements MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> map(Tuple2<Integer, Point> t) {
            return new Tuple3<>(t.f0, t.f1, 1L);
        }
    }


    public static final class Centroid_aggregate implements ReduceFunction<Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
            return new Tuple3<>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
        }
    }


    public static final class Cal_centroid implements MapFunction<Tuple3<Integer, Point, Long>, Center> {

        @Override
        public Center map(Tuple3<Integer, Point, Long> value) {
            return new Center(value.f0, value.f1.div(value.f2));
        }
    }
}
