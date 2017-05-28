/**
 * Created by Chaomin on 5/25/2017.
 */
import java.io.*;
import java.util.*;

public class sort_file_test_2 {
    final  String input_dir = "C:/Users/Chaomin/Desktop/assignment2_data/temp/task2_result.csv";
    final  String output_dir = "C:/Users/Chaomin/Desktop/assignment2_data/result/task2_result.csv";

    public  void main(String[] args) throws Exception {
        sort_file (this.input_dir, this.output_dir);

    }

    public static void sort_file(String input_address, String output_address) throws FileNotFoundException, IOException{

        HashMap<String, String> a = new HashMap<>();

        BufferedReader input = new BufferedReader(new FileReader(input_address));
        String line = input.readLine();

        while (line != null) {
            String columns [] = line.split("\\,\\d+\\d\\,");

            a.put(columns[0], columns[1]);
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


        FileWriter writer = new FileWriter(output_address);

        for (Map.Entry<String,String> j : List) {
            writer.append(j.getKey())
                    .append(",")
                    .append(j.getValue())
                    .append("\n");
        }


        writer.close();
        System.out.println(List);
    }
}
