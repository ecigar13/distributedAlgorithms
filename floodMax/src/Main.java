import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.IdentityScope;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.omg.CORBA.PUBLIC_MEMBER;

import floodMax.FloodMaxImplementation;
import jdk.internal.dynalink.beans.StaticClass;
import message.Message;


public class Main {

  public static void main(String args[]) throws IOException {
    // read from file
    Scanner in = new Scanner(new FileReader("graph.txt"));
    //first entry of the graph.txt provides the number of slave nodes
    int size = in.nextInt() + 1;
    System.out.println("Size is : "+size);
    int[] node_ids = new int[size];
    int[][] matrix = new int[size][size];
    
    //Hashmap to store message between nodes
    ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> common_map = new ConcurrentHashMap<Integer,LinkedBlockingQueue<Message>>();
    
    //assigning ids to all the nodes
    node_ids[0]=0;
    for (int i = 1; i < size; i++) 
    {
      node_ids[i] = in.nextInt();
      System.out.println(i+" : "+node_ids[i]);
    }

    // 0 is master node id; 1 to n is the slave nodes
    //every slave is connected to the master node
    
    for (int i = 0; i < size; i++) 
    {
      for (int j = 0; j < size; j++) 
      {
        matrix[i][j] = in.nextInt();
        //System.out.println(matrix[i][j]);
      }
    }

    System.out.println("Test print matrix");
    for (int i = 0; i < size; i++) {
      for (int j = 0; j < size; j++) {
        System.out.print(matrix[i][j] + " ");
      }
      System.out.println();
    }

    // implement floodmax here.
    //FloodMaxImplementation algo =new FloodMaxImplementation(size, node_ids, matrix,common_map) ;
    Thread t1 = new Thread(new FloodMaxImplementation(size, node_ids, matrix,common_map));
    t1.start();

    //prints final output tree
   /* PrintWriter out = new PrintWriter(new FileWriter("graphOut.txt"));
    out.print(algo.getLeader());
    out.close();*/
    in.close();
  }

}
