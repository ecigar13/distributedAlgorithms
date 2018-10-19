import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import floodMax.FloodMaxImplementation;
import message.Message;

public class Main {

  public static void main(String args[]) throws IOException {
    // read from file
    Scanner in = new Scanner(new FileReader("graph4.txt"));

    // first entry of the graph.txt provides the number of slave nodes
    // adj matrix's first row and column is the master node.
    int size = in.nextInt();
    System.out.println("Size is : " + size);
    int[] vertexIdArray = new int[size];
    int[][] adjMatrix = new int[size][size];

    // Hashmap: integer is assigned sequentially (not node id), queue of message.
    ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> idAndMsgQueueMap = new ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>>();

    // master node's id is 0
    vertexIdArray[0] = 0;
    // assigning ids to all the nodes
    for (int i = 0; i < size; i++) {
      vertexIdArray[i] = in.nextInt();
      System.out.print(vertexIdArray[i] + " ");
    }

    // 0 is master node id; 1 to n is the slave nodes
    // every slave is connected to the master node

    for (int i = 0; i < size; i++) {
      for (int j = 0; j < size; j++) {
        adjMatrix[i][j] = in.nextInt();
      }
    }

    System.out.println("\nTest print matrix");
    for (int i = 0; i < size; i++) {
      for (int j = 0; j < size; j++) {
        System.out.print(adjMatrix[i][j] + " ");
      }
      System.out.println();
    }

    // implement floodmax here.
    Thread t1 = new Thread(new FloodMaxImplementation(size, vertexIdArray, adjMatrix, idAndMsgQueueMap));
    t1.start();

    in.close();
  }

}
