import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

public class Main {

  public static void main(String args[]) throws IOException {
    // read from file
    String fileName = "graph";
    Scanner in = new Scanner(new FileReader(fileName + ".txt"));

    // first entry of the graph.txt provides the number of slave nodes
    // adj matrix's first row and column is the master node.
    int size = in.nextInt();
    System.out.println("Size is : " + size);
    int[] vertexIdArray = new int[size];
    int[][] adjMatrix = new int[size][size];

    // assigning ids to all the nodes
    for (int i = 0; i < size; i++) {
      vertexIdArray[i] = in.nextInt();
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
    System.out.println("Print double matrix and write to file: ");
    double[][] adjDoubleMatrix;
    try {
      adjDoubleMatrix = writeMatrixToFile(size, vertexIdArray, adjMatrix, 100, fileName);
      for (int i = 0; i < size; i++) {
        for (int j = 0; j < size; j++) {
          System.out.print(String.format("%.1f\t", adjDoubleMatrix[i][j]));
        }
        System.out.println();
      }

      // implement floodmax here.
      Thread t1 = new Thread(new MasterThread(size, vertexIdArray, adjDoubleMatrix));
      t1.setName("thread_0");
      t1.start();
    } catch (Exception e) {
      e.printStackTrace();
    }

    in.close();
  }

  /**
   * Convert 0 1 matrix to matrix with random distance and -1 for no connection.
   * 
   * @param size
   * @param vertexIdArray
   * @param adjMatrix
   * @param max
   * @param fileName
   * @return
   * @throws IOException
   */
  public static double[][] writeMatrixToFile(int size, int[] vertexIdArray, int[][] adjMatrix, int max, String fileName)
      throws IOException {
    double[][] adjDoubleMatrix = new double[size][size];
    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName + "-ghs.txt"));
    Random r = new Random();
    writer.write(String.format("%d", size));
    writer.newLine();
    for (int i = 0; i < size; i++) {
      writer.write(String.format("%d ", vertexIdArray[i]));
    }
    writer.newLine();
    for (int i = 0; i < size; i++) {
      for (int j = 0; j < size; j++) {
        if (adjMatrix[i][j] == 0) {
          writer.write(String.format("%3.1f  ", -1.0));
          adjDoubleMatrix[i][j] = -1;
        } else {
          double temp = r.nextDouble() * max;
          writer.write(String.format("%3.1f  ", temp));
          adjDoubleMatrix[i][j] = temp;
        }
      }
      writer.newLine();
    }
    writer.flush();
    writer.close();
    return adjDoubleMatrix;

  }

}
