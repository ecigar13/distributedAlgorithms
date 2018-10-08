package floodMax;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.sun.prism.PhongMaterial.MapType;

import message.*;
import message.Message.MessageType;

public class SlaveThread implements Runnable {

  protected int id;
  private MasterThread masterNode;
  private SlaveThread parent;
  protected int leaderId = 0;
  protected int diam = 0;
  protected int round = 0;
  protected boolean newInfo = true;
  protected MessageType status;

  protected Map<Integer, Integer> distance = new ConcurrentHashMap<Integer, Integer>();
  protected Map<Integer, SlaveThread> neighbors = new ConcurrentHashMap<Integer, SlaveThread>();
  protected Queue<Message> nextRoundMsg = new LinkedBlockingQueue<Message>();
  protected Queue<Message> thisRoundMsg = new LinkedBlockingQueue<Message>();

  public void sendMessage(Message msg) {
    nextRoundMsg.add(msg);
  }

  public enum RoundDone {
    YES, NO;
  }

  protected RoundDone roundFinishStatus = RoundDone.YES;
  protected boolean suspendStatus;

  /**
   * Implement find diameter function here. Not finished
   */
  public void diameter() {
    // after edge rounds, done.

    // First, broadcast distance to all possible nodes.
    for (Map.Entry<Integer, SlaveThread> n : neighbors.entrySet()) {
      if (parent != n.getValue())
        n.getValue().sendMessage(new Message(id, id, diam, MessageType.DIAMETER));
    }
    // then process incoming messages
    // All processes
    // upon receiving d from p:
    // if d+1 < distance:
    // distance := d+1
    // parent := p
    // send distance to all neighbors
  }

  /**
   * Create a new Message object for each neighbor and send to all of them. Because the messages can be edited later.
   * @param m
   */
  protected void broadcastToNeighbors(Message m) {
    for (Map.Entry<Integer, SlaveThread> pair : neighbors.entrySet()) {
      // broadcast to neighbors. Don't broadcast to parents
      if (pair.getKey() != parent.getId())
        pair.getValue().sendMessage(new Message(m.getSenderId(), m.getFrom(), m.getDistanceFromTo(), m.getmType()));
    }
  }

  protected void processDiameterMessage(Message message) {
    // msg is guanrantee to be MessageType.DIAMETER

    // if the distance I get is smaller than my distance to that node
    // or I've never seen that vertex's id before
    if (message.getDistanceFromTo() + 1 < distance.get(message.getFrom()) || distance.get(message.getFrom()) == null) {
      distance.put(message.getFrom(), message.getDistanceFromTo() + 1);
      parent = neighbors.get(message.getSenderId());
      // broadcast distance to all neighbords
      // reuse the incoming message
      message.setSenderId(id);
      message.setDistanceFromTo(message.getDistanceFromTo() + 1);
      broadcastToNeighbors(message);
    }
    // else, don't do anything.

  }

  /**
   * Process different messages in the queue
   */
  protected void processMessage(Message message) {
    if (message.getmType() == MessageType.EXPLORE) {
      if (message.getMessageUid() > leaderId) {
        leaderId = message.getMessageUid();
        newInfo = true;
      } else
        newInfo = false;
    } else if (message.getmType() == MessageType.NACK) {
      // implement

    } else if (message.getmType() == MessageType.REJECT) {
      // implement
    } else if (message.getmType() == MessageType.DIAMETER) { // if this message is for finding diameter
      processDiameterMessage(message);
    } else
      System.err.println("This message cannot be processed: " + message.getmType().toString());

  }

  /**
   * Implement Floodmax here. Not finished
   */
  protected void floodMax() {
    round += 1;
    for (Message m : thisRoundMsg) {
      processMessage(m);
    }

    if (round == diam) {
      if (leaderId == id) {
        masterNode.setLeaderId(leaderId);
        status = MessageType.IAMLEADER;
        System.out.println("Leader: " + id + " " + status.toString());
        // send message to master
      } else
        status = MessageType.NOTLEADER;

      if (round < diam && newInfo == true) {

        for (Map.Entry<Integer, SlaveThread> pair : neighbors.entrySet()) {
          pair.getValue().sendMessage(new Message(id, leaderId, MessageType.EXPLORE, round));
        }
      }
    }
  }

  @Override
  public synchronized void run() {
    System.out.println("I RAN!!!");
    System.out.println("Neighbors: " + neighbors.size());
    // while status is FIND DIAMETER, do the find diameter part

    // set status to FIND FLOODMAX
    // while status is FIND FLOODMAX, do the find floodmax part

    // set status to DONE

    System.err.println("The thread will now die");

  }

  void suspend() {
    suspendStatus = true;
  }

  public boolean resume() {
    if (roundFinishStatus.equals(RoundDone.YES)) {
      suspendStatus = false;
      notify();
      return true;
    } else
      return false;
  }

  public MasterThread getMasterNode() {
    return masterNode;
  }

  public void setMasterNode(MasterThread masterNode) {
    this.masterNode = masterNode;
  }

  public SlaveThread getParent() {
    return parent;
  }

  public void setParent(SlaveThread parent) {
    this.parent = parent;
  }

  public Map<Integer, SlaveThread> getNeighbors() {
    return neighbors;
  }

  public void setNeighbors(Map<Integer, SlaveThread> neighbors) {
    this.neighbors = neighbors;
  }

  public RoundDone getStatus() {
    return roundFinishStatus;
  }

  public void setStatus(RoundDone status) {
    this.roundFinishStatus = status;
  }

  public RoundDone isFinished() {
    return getStatus();
  }

  public void setFinished(RoundDone finished) {
    this.roundFinishStatus = finished;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public int getLeaderId() {
    return leaderId;
  }

  public void setLeaderId(int leaderId) {
    this.leaderId = leaderId;
  }

  public int getRound() {
    return round;
  }

  public void setRound(int round) {
    this.round = round;
  }

  public void setDiam(int diam) {
    this.diam = diam;
  }

  /**
   * Empty constructor for subclass.
   */
  public SlaveThread() {
  }

  /**
   * Constructor.
   * 
   * @param id
   * @param masterNode
   */
  public SlaveThread(int id, MasterThread masterNode) {
    this.id = id;
    this.masterNode = masterNode;

  }

}
