import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.FileSystem;

import java.util.*;

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



            DataSet<Tuple4<Integer, Double, Double, Double>> center_ids = env.readCsvFile(input_last_centerid_file_dir)
                    .includeFields("1111")
                    .types(Integer.class, Double.class, Double.class, Double.class);

            DataSet<Tuple7<Integer, Double, Double, Double, Double, Double, Double>> joined_points_and_centerid = label_points
                    .join(center_ids)
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

                    //System.out.print("error_of_points");
                    //error_of_points.print();

            DataSet<Tuple2<Integer, ArrayList<Double>>> sorted_error_of_points = error_of_points
                    .groupBy(0)
                    .reduceGroup((tuples, out) -> {
                        int cluster_id = -1;
                        ArrayList<Double> error_List = new ArrayList<>();
                        ArrayList<Double> sorted_error_List = new ArrayList<>();

                        for(Tuple5<Integer, Double, Double, Double, Double> tuple : tuples) {
                             cluster_id = tuple.f0;
                             error_List.add(tuple.f4);

                        }

                        Collections.sort(error_List);

                        for (int i = 0; i < (0.9 * error_List.size()); i++) {
                            sorted_error_List.add(error_List.get(i));
                        }

                        out.collect(new Tuple2<>(cluster_id, sorted_error_List));


                    })
                    .returns(new TupleTypeInfo(TypeInformation.of(Integer.class), TypeInformation.of(ArrayList.class)));

            //System.out.print("sorted_error_of_points");
            //sorted_error_of_points.print();

            DataSet<Tuple2<Integer,Double>> sorted_error_of_points_2 = sorted_error_of_points
                    .flatMap((tuple, out) -> {
                        int cluster_id = -1;
                        cluster_id = tuple.f0;
                        ArrayList List = tuple.f1;
                        Double error = 0.0;


                        for (int i = 0; i < (List.size()); i++) {

                            error = (Double)List.get(i);
                            out.collect(new Tuple2<>(cluster_id, error));
                        }

                    })
                    .returns(new TupleTypeInfo(TypeInformation.of(Integer.class), TypeInformation.of(Double.class)));


            DataSet<Tuple3<Double, Double, Double>> filtered_points = sorted_error_of_points_2
                    .join(error_of_points)
                    .where(0,1)
                    .equalTo(0,4)
                    .projectSecond(1,2,3);



            DataSet<Point> points = filtered_points
                    .map(new K_means.collect());

            //generate dataset centroid_points
            int k = 5; //defulet k = 5
            int iterate_size = 10; //defult iteration = 10
            if(args.length>4){
                k= Integer.parseInt(args[4]);
            }
            if(args.length>5){
                iterate_size = Integer.parseInt(args[5]);
            }
            List<Center> genCenter = new ArrayList<Center>();

            for(int i = 0; i<k; i++){
                double ly6c = Math.random()*5;
                double CD11b = Math.random()*5;
                double SCA1 = Math.random()*5;
                int c = i+1;

                genCenter.add(new Center(ly6c,CD11b,SCA1,c));

            }

            DataSet<Center> centroids = env.fromCollection(genCenter);

            //set number of bulk iterations
            IterativeDataSet<Center> first_centroids = centroids.iterate(iterate_size);

            DataSet<Center> new_center_id = points
                    .map(new K_means.Select_Centroid()).withBroadcastSet(first_centroids, "centerids")
                    .map(new K_means.Count())
                    .groupBy(0).reduce(new K_means.Centroid_aggregate())
                    .map(new K_means.Cal_centroid());


            // put new_center_id into next iteration
            DataSet<Center> last_centroids = first_centroids.closeWith(new_center_id);

            DataSet<Tuple2<Integer, Point>> labeled_points = points
                    .map(new K_means.Select_Centroid()).withBroadcastSet(last_centroids, "centerids");


            //labeled_points.print();

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

            //final_result.print();
            final_result.writeAsCsv(output_task3_result_dir, "\n", ",",  FileSystem.WriteMode.OVERWRITE).setParallelism(1);
            env.execute();
            K_means.sort_file(args[2], args[3]);
        }

        else{
            System.out.println("Wrong input parameters!");
            System.out.println("End of the program!");
            System.exit(0);
        }

    }
}

