package floodMax;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MasterNode extends SlaveNode implements Runnable {

  private int size;
  private int round;
  private int[] ids;
  private int[][] matrix;
  private Map<Integer, SlaveNode> m = new ConcurrentHashMap<Integer, SlaveNode>();

  public MasterNode(int size, int[] ids, int[][] matrix) {
    this.size = size;
    this.ids = ids;
    this.matrix = matrix;

    for (int i = 0; i < size; i++) {
      SlaveNode slaveNode = new SlaveNode();
      m.put(this.ids[i], slaveNode);
    }
  }

  public SlaveNode getSlave(int slaveId) {
    return m.get(slaveId);
  }

  /**
   * Go through the hashmap and determine if the round is finished.
   * 
   * @return
   */
  public synchronized boolean roundFinished() {
    boolean done = false;
    for (Map.Entry<Integer, SlaveNode> node : m.entrySet()) {
      done &= !node.getValue().getStatus().equals(Start.YES);
      if (done == false) {
        return false;
      }
    }
    return done;
  }

  public synchronized void setStartStatus() {
    for (Map.Entry<Integer, SlaveNode> node : m.entrySet()) {
      node.getValue().setStatus(Start.YES);
    }
  }

  public synchronized void suspendAll() {
    System.out.println("Suspending all slaves. Round: " + this.round);
    for (Map.Entry<Integer, SlaveNode> node : m.entrySet()) {
      node.getValue().suspend();
    }
  }

  public synchronized void resumeAll() {
    System.out.println("Resuming all slaves. Round: " + this.round);
    for (Map.Entry<Integer, SlaveNode> node : m.entrySet()) {
      node.getValue().resume();
    }
  }

  @Override
  public void run() {

    // TODO Auto-generated method stub

    // create master
    // create slaves
  }

}
