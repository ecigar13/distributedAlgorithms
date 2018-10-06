import java.io.*;
import java.util.*;
import java.lang.*;
import java.util.HashMap;
import java.util.Queue;
import FloodMax.message;


public class Storage {

  // HashMap for all the nodes in the system holding queue for each node
  public static Map<Integer, data> map = Collections.synchronizedMap(new HashMap<Integer, data>());

  class Data {
    // int node;
    Queue<message> q = new Queue<message>();

    public Data(message message) {
      // this.node = node;
      q.push(message);
    }
  }
}
