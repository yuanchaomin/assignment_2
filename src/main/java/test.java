import org.apache.commons.collections.map.MultiValueMap;

import java.util.*;

/**
 * Created by Chaomin on 5/19/2017.
 */
public class test {
    public static void main(String[] args) {
        MultiValueMap dict = new MultiValueMap();
        dict.put(1,10);
        dict.put(1,9);
        dict.put(1,5);
        dict.put(2,6);
        dict.put(2,7);
        dict.put(3,9);
        dict.put(3,10);

        List<MultiValueMap.Entry<Integer, Integer >> list =
                new LinkedList<MultiValueMap.Entry<Integer, Integer >>(dict.entrySet());
                Collections.sort(list, new Comparator<HashMap.Entry<Integer, Integer>>() {
                    @Override
                    public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
                        return (o1.getValue()).compareTo(o2.getValue());
                    }
                });


        Map<Integer,Integer> result = new MultiValueMap();
        for (Map.Entry<Integer,Integer> entry : list){
            result.put( entry.getKey(), entry.getValue());
        }

        System.out.println(result);
    }



}
