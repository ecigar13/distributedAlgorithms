import java.util.*;
import java.io.IOException;
import java.lang.Exception;
import java.util.ArrayList;

public class FloodMax {
  public static void main(String args[]) {

    static int round = 0;
    int number_of_nodes = 4;
    Storage str = new Storage();
    int[][] adj_matrix = new int[][] { { 1, 1, 1, 1, 1 }, { 1, 0, 1, 1, 0 }, { 1, 1, 0, 0, 1 }, { 1, 1, 0, 0, 1 },
        { 1, 0, 1, 1, 0 } };
    int Master_UID = 0;

    new Master.start(Master_UID, str, round, number_of_nodes);

    for (int uid = 1; uid <= 4; i++) {
      new nodes.start(uid, str, adj_matrix, number_of_nodes);
    }

    public class message {
      int UID;
      int max_UID;
      int round;
      String type;
      ArrayList<Integer> neighbour_nodes = new ArrayList<Integer>();

      message(int uid, int max_uid, int round, String type) {
        this.UID = uid;
        this.max_UID = max_uid;
        this.round = round;
        this.type = type;
      }

      message(int neighbour) {
        neighbour_nodes.add(neighbour);
      }

      String type() {
        return this.type;
      }

      int round() {
        return this.round;
      }

      int uid() {
        return this.UID;
      }

      int max_UID() {
        return this.max_UID;
      }

    }
    ;
  }
}
