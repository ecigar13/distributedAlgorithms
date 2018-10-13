package floodMax;

import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import message.Message;

/**
 * MasterNode is a special case of a SlaveNode.
 * 
 * @author khoa
 *
 */
public class MasterThread extends SlaveThread {
  public static ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> globalIdAndMsgQueueMap;

  // max id to be in sync with message class object. Not needed here. Junk value
  protected int masterId = 0;
  protected int size;
  protected int[] neighborArray;
  protected int[][] matrix;

  protected boolean masterMustDie = false;

  // master will put information about the round in this hash map which is
  // accessible to all
  // hash Map for storing children pointers
  private int numberOfFinishedThreads;
  private ArrayList<SlaveThread> threadList = new ArrayList<SlaveThread>();
  protected LinkedBlockingQueue<Message> localMessageQueue = new LinkedBlockingQueue<>();

  /**
   * Constructor
   * 
   * @param size
   * @param neighbors
   * @param matrix
   */
  public MasterThread(int size, int[] neighbors, int[][] matrix) {
    this.round = 0;
    this.numberOfFinishedThreads = 0;
    this.newInfo = true;
    this.size = size;
    this.neighborArray = neighbors;
    this.matrix = matrix;

    // put master into concurrent hash map
    MasterThread.globalIdAndMsgQueueMap = new ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>>();

    fillLocalMessagesToSend();
  }

  @Override
  public void fillLocalMessagesToSend() {
    System.err.println("Filling localMessagesToSend.");
    localMessagesToSend.put(0, new LinkedBlockingQueue<Message>());
    for (int i : neighborArray) {
      // add signal to start
      localMessageQueue = new LinkedBlockingQueue<Message>();
      localMessagesToSend.put(i, localMessageQueue);
    }
  }

  @Override
  public void run() {
    System.err.println("The Master has started. Size: " + size);

    createThreads();
    setNeighbors();
    fillGlobalQueue();
    checkGlobalQueueNotEmpty();

    while (!masterMustDie) {
      sleep();
      // master gets it's own queue from the outside hashmap, with id 0.
      globalIdAndMsgQueueMap.get(0).drainTo(localMessageQueue);

      while (!(localMessageQueue.isEmpty())) {
        System.out.println("Master checking its queue. Size of queue is: " + localMessageQueue.size());

        Message tempMsg = localMessageQueue.poll();

        if (tempMsg.getmType().equalsIgnoreCase("Leader")) {

          // if a node says it's Leader to master, master tells the node to terminate.
          // terminate is a static variable, so all threads will receive the signal.

          // empty the remaining msg in master's q.
          while (!localMessageQueue.isEmpty()) {
            localMessageQueue.poll();
          }

          // send terminate msg back to the sender.
          try {
            globalIdAndMsgQueueMap.get(tempMsg.getSenderId())
                .put(new Message(id, this.round, this.myMaxUid, "Terminate"));
            System.out.println("Telling the master to die.");
            masterMustDie = true;
            break;
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        // if a thread says it's done, then master increase the Done count.
        else if ((tempMsg.getmType().equals("Done"))) {
          numberOfFinishedThreads++;
          // all slaves completed the round
          if (numberOfFinishedThreads == size - 1) {

            round++;

            // if all slaves completed the round, master send messages to all nodes to start
            // next round.
            for (int i = 1; i < size; i++) {
              Message msg = new Message(id, round + 1, myMaxUid, "Round_Number");
              try {
                localMessagesToSend.get(i).put(msg);
              } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
              }
            }
            // Reset Done Count for next round messages
            numberOfFinishedThreads = 0;
          }
        }
      }
      sendRoundStartMsg();
      pushToGlobalQueue();
      // run threads
      startAllThreads();
    }

  }

  /**
   * Create all nodes/threads, set their names and start them.
   */
  public synchronized void createThreads() {
    try {
      for (int nodeIndex = 1; nodeIndex < size; nodeIndex++) {
        SlaveThread t = new SlaveThread(neighborArray[nodeIndex], this, globalIdAndMsgQueueMap);
        threadList.add(t);
      }
      System.err.println("Created threads. ");
    } catch (Exception err) {
      err.printStackTrace();
    }
  }

  public synchronized void startAllThreads() {
    for (SlaveThread t : threadList) {
      t.run();
    }
  }

  /**
   * Implement. Set an array of neighbor id -> use it to find queue in global
   * queue.
   */
  public synchronized void setNeighbors() {
    for (SlaveThread t : threadList) {
      for (int i = 1; i < size; i++) {
        if (neighborArray[i] != 0) {
          t.insertNeighbour(neighborArray[i]);
          // System.out.println(ids[i]);
        }
      }
    }
  }

  /**
   * First step of master thread: fill the global queue.
   */
  public void fillGlobalQueue() {
    System.err.println("Filling global queue.");
    globalIdAndMsgQueueMap.put(0, new LinkedBlockingQueue<Message>());
    for (int i : neighborArray) {
      // add signal to start
      localMessageQueue = new LinkedBlockingQueue<Message>();
      globalIdAndMsgQueueMap.put(i, localMessageQueue);
    }
  }

  public void sendRoundStartMsg() {
    for (int i : neighborArray) {
      System.out.println("Send Round_Number msg to " + i);
      try {
        localMessagesToSend.get(i).put(new Message(id, round, id, "Round_Number"));

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public void checkGlobalQueueNotEmpty() {
    for (Entry<Integer, LinkedBlockingQueue<Message>> p : globalIdAndMsgQueueMap.entrySet()) {
      System.out.println(p.getKey() + " " + p.getValue());
    }
  }
}
