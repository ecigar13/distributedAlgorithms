import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.IdentityScope;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.Vector;

import floodMax.FloodMaxImplementation;

public class Main {

  public static void main(String args[]) throws IOException {
    // read from file
    Scanner in = new Scanner(new FileReader("graph.txt"));
    int size = in.nextInt();
    int[] ids = new int[size];
    int[][] matrix = new int[size][size];

    for (int i = 0; i < size; i++) {
      ids[i] = in.nextInt();
      System.err.println(ids[i]);
    }

    for (int i = 0; i < size; i++) {
      for (int j = 0; j < size; j++) {
        matrix[i][j] = in.nextInt();
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
    FloodMaxImplementation algo = new FloodMaxImplementation(size, ids, matrix);
    algo.run();

    PrintWriter out = new PrintWriter(new FileWriter("graphOut.txt"));
    out.print(algo.getLeader());
    out.close();
    in.close();
  }

}
