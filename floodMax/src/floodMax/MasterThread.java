package floodMax;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import message.Message;
import message.Message.MessageType;

/**
 * MasterNode is a special case of a SlaveNode.
 * 
 * @author khoa
 *
 */
public class MasterThread extends SlaveThread {

  private int size;
  private int[] ids;
  private int[][] matrix;
  private Map<Integer, SlaveThread> slaves = new ConcurrentHashMap<Integer, SlaveThread>();

  /**
   * Constructor
   * 
   * @param size
   * @param ids
   * @param matrix
   */
  public MasterThread(int size, int[] ids, int[][] matrix) {
    this.size = size;
    this.ids = ids;
    this.matrix = matrix;
    this.roundFinishStatus = RoundDone.NO;

    // create SlaveNodes and add to slaves map
    for (int i = 0; i < size; i++) {
      SlaveThread slaveNode = new SlaveThread(ids[i], this);
      slaves.put(this.ids[i], slaveNode);
    }

    setNeighbors();
  }

  @Override
  protected synchronized void processMessage(Message message) {
    if (message.getmType() == MessageType.IAMLEADER) {
      if (message.getMessageUid() > leaderId) {
        leaderId = message.getMessageUid();
        newInfo = true;
      } else
        newInfo = false;

    } else
      System.err.println("This message cannot be processed: " + message.getmType().toString());
  }

  /**
   * Go through the slaves map. For each slave, use the matrix[][] to find its
   * neighbor, then get it from slaves map and add it to a HashMap. Finally, call
   * setNeighbor function of slaves.
   * 
   * Inefficient in large graph. I might implement Connection instead.
   */
  private void setNeighbors() {
    // set neighbors of slaves
    // I didn't think this through, so check for bidirectional bonds
    for (int i = 0; i < size; i++) {
      Map<Integer, SlaveThread> neighbors = new ConcurrentHashMap<Integer, SlaveThread>();

      // add nodes with their neighbors on here.
      for (int j = 0; j < size; j++) {
        if (this.matrix[i][j] != 0) {
          neighbors.put(this.ids[j], slaves.get(this.ids[j]));
        }
      }

      // then set all neighbors to SlaveNodes
      slaves.get(this.ids[i]).setNeighbors(neighbors);
    }
    System.err.println(slaves.size());
  }

  public SlaveThread getSlave(int slaveId) {
    return slaves.get(slaveId);
  }

  /**
   * Go through the hashmap and determine if the round is finished.
   * 
   * @return
   */
  public synchronized boolean roundFinished() {
    boolean done = false;
    for (Map.Entry<Integer, SlaveThread> node : slaves.entrySet()) {
      done &= !node.getValue().getStatus().equals(RoundDone.YES);
      if (done == false) {
        return false;
      }
    }
    return done;
  }

  public synchronized void setStartStatus() {
    for (Map.Entry<Integer, SlaveThread> node : slaves.entrySet()) {
      node.getValue().setStatus(RoundDone.YES);
    }
  }

  public synchronized void suspendAll() {
    System.out.println("Suspending all slaves. Round: " + this.round);
    for (Map.Entry<Integer, SlaveThread> node : slaves.entrySet()) {
      node.getValue().suspend();
    }
  }

  public synchronized void resumeAll() {
    System.out.println("Resuming all slaves. Round: " + this.round);
    for (Map.Entry<Integer, SlaveThread> node : slaves.entrySet()) {
      node.getValue().resume();
    }
  }

  /**
   * Set diameter for all slave nodes
   */
  @Override
  public synchronized void setDiam(int diam) {
    super.setDiam(diam);
    for (Map.Entry<Integer, SlaveThread> node : slaves.entrySet()) {
      node.getValue().setDiam(diam);
    }

  }

  /**
   * Only test if the threads are created at this point.
   */
  @Override
  public void run() {

    // while(round < diam) {
    for (Map.Entry<Integer, SlaveThread> s : slaves.entrySet()) {
      System.out.println(s.getValue().getId());
      s.getValue().run();
    }

    // }

    System.err.println("The master will now die");

    // create master
    // create slaves
  }

}
