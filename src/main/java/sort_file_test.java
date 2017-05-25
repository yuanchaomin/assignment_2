import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Chaomin on 5/25/2017.
 */
public class sort_file_test {
    final  String input_dir = "C:/Users/Chaomin/Desktop/assignment2_data/temp/task1_result.csv";
    final  String output_dir = "C:/Users/Chaomin/Desktop/assignment2_data/result/task1_result.csv";

    public  void main(String[] args) throws Exception {
            sort_file (this.input_dir, this.output_dir);

    }

    public static void sort_file(String input_address, String output_address) throws FileNotFoundException, IOException{

        HashMap<String, Integer> a = new HashMap<String, Integer>();
        HashMap<String, Integer> b = new HashMap<String, Integer>();



        BufferedReader input = new BufferedReader(new FileReader(input_address));
        String line = input.readLine();

        while (line != null) {
            String columns [] = line.split(",");
            a.put(columns[0], Integer.parseInt(columns[1]));
            line = input.readLine();
        }

        ArrayList<Map.Entry<String,Integer>> List = new ArrayList<Map.Entry<String,Integer>>(a.entrySet());
        //Comparator<Integer> comparator = Collections.reverseOrder();
        Collections.sort(List, new Comparator<Map.Entry<String,Integer>>()
        {
            public int compare( Map.Entry<String,Integer> o1, Map.Entry<String,Integer> o2 ) {
                return (o2.getValue()).compareTo(o1.getValue() );
                }
        } );


        FileWriter writer = new FileWriter(output_address);

        for (Map.Entry<String,Integer> j : List) {
            writer.append(j.getKey())
                    .append(",")
                    .append(j.getValue().toString())
                    .append("\n");
        }


        writer.close();
        System.out.println(List);
    }
}
