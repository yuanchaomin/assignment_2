import java.util.*;

import javafx.util.Pair;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.com.google.common.collect.LinkedListMultimap;

/**
 * Created by Chaomin on 5/19/2017.
 */
public class test {
    public static void main(String[] args) {
        LinkedListMultimap dict_with_list = LinkedListMultimap.create();

        dict_with_list.put(1,2);
        dict_with_list.put(1,3);
        dict_with_list.put(1,5);
        dict_with_list.put(2,7);
        dict_with_list.put(2,9);
        dict_with_list.put(2,10);

        List<Map.Entry<Integer, Integer>> list= new LinkedList<Map.Entry<Integer, Integer>>(dict_with_list.entries());
        Collections.sort(list, new Comparator<HashMap.Entry<Integer, Integer>>() {
            @Override
            public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
                return (o1.getValue()).compareTo(o2.getValue());
            }
        });

        LinkedListMultimap result = LinkedListMultimap.create();
        HashMap <Integer, List> result2 = new HashMap<>();

        for (Map.Entry<Integer,Integer> entry : list){
            result.put( entry.getKey(), entry.getValue());
        }


        List<Integer> one_list = new LinkedList(result.get(1)).subList(0,2);

        result2.put(1, one_list);





        System.out.println(result2);
    }

    public static class Put_data_to_multimap implements MapFunction<Tuple5<Integer, Double, Double, Double, Double>, Pair> {

        public Pair map(Tuple5<Integer, Double, Double, Double, Double> t) throws Exception {
            return new Pair(t.f0, t.f4);
        }
    }

    public static class Collect_error_list extends RichMapFunction<Pair, LinkedListMultimap> {
        private Collection<Pair> slice_number;
        private LinkedList<Pair> slice_number_list;


        public void open(Configuration parameters) {
            this.slice_number = getRuntimeContext().getBroadcastVariable("slice_number");

        }



        public LinkedListMultimap map(Pair p) throws  Exception {


            LinkedListMultimap dict = LinkedListMultimap.create();
            dict.put(p.getKey(), p.getValue());

            List<Map.Entry<Integer, Integer>> list= new LinkedList<Map.Entry<Integer, Integer>>(dict.entries());
            Collections.sort(list, new Comparator<HashMap.Entry<Integer, Integer>>() {
                @Override
                public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
                    return (o1.getValue()).compareTo(o2.getValue());
                }
            });

            LinkedListMultimap result = LinkedListMultimap.create();
            LinkedListMultimap final_result = LinkedListMultimap.create();
            HashMap <Integer, List> result2 = new HashMap<>();

            for (Map.Entry<Integer,Integer> entry : list){
                result.put( entry.getKey(), entry.getValue());
            }

            int k_length = slice_number.size();

            List<Pair> slice_number_list = new ArrayList<>(this.slice_number);

            for (int i = 0; i<k_length; i++) {

                result2.put(i, new LinkedList(result.get(i)).subList(0,1 + (int)slice_number_list.get(i).getValue()));
            }


            for (Map.Entry<Integer,List> entry : result2.entrySet()) {

            }


            return dict;
        }
    }




}
