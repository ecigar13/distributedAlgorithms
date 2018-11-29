
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * MasterNode is a special case of a SlaveNode.
 * 
 * @author khoa
 *
 */
public class MasterThread extends SlaveThread {

  protected int masterId = 0;
  protected int size;
  protected int[] slaveArray;
  protected double[][] matrix;

  protected int noMwoeCount = 0;
  protected boolean masterMustDie = false;

  private int numberOfFinishedThreads;
  protected ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> globalIdAndMsgQueueMap;
  private ArrayList<SlaveThread> threadList = new ArrayList<SlaveThread>();

  protected HashSet<Integer> leaderSet = new HashSet<>();

  /**
   * Constructor
   * 
   * @param size
   * @param slaveArray
   * @param matrix
   */
  public MasterThread(int size, int[] slaveArray, double[][] matrix) {
    this.round = 1;
    this.numberOfFinishedThreads = 0;
    this.size = size;
    this.slaveArray = slaveArray;
    this.matrix = matrix;
    this.name = "Master";
    this.id = 0;
    this.level = 0;
    this.coreLink = null;

    this.globalIdAndMsgQueueMap = new ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>>();
    initGlobalIdAndMsgQueueMap();
  }

  /**
   * First step of master thread: fill the global queue.
   */
  public void initGlobalIdAndMsgQueueMap() {
    globalIdAndMsgQueueMap.put(0, new LinkedBlockingQueue<Message>());
    for (int i : slaveArray) {
      globalIdAndMsgQueueMap.put(i, new LinkedBlockingQueue<Message>());
    }
  }

  @Override
  public void run() {
    System.out.println("The Master has started. Size: " + size);
    createThreads();
    sendRoundStartMsg();
    for (int i : slaveArray) {
      leaderSet.add(i);
    }

    do {
      globalIdAndMsgQueueMap.get(id).drainTo(localMessageQueue);
      System.out.printf("\nMaster checking its queue. Size of queue is: %d round %d \n", localMessageQueue.size(),
          round);

      while (!(localMessageQueue.isEmpty())) {
        try {
          Message tempMsg = localMessageQueue.take();
          boolean hasBasicEdge = false;
          for (SlaveThread t : threadList) {
            if (t.basicEdge.size() != 0)
              hasBasicEdge = true;
          }
          if (leaderSet.size() == 1 && !hasBasicEdge) {
            // if there is only one leader, then algorithm is complete. call killAll()
            localMessageQueue.clear();

            System.err.println("---Telling the master to die. Leader is: " + tempMsg.getSenderId() + " round " + round);
            masterMustDie = true;
            coreLink = tempMsg.getCore();
            killAll();
          } else if ((tempMsg.getmType().equals("Done"))) {
            processRoundDoneMsg(tempMsg);
            numberOfFinishedThreads++;
          }
          // all slaves completed the round
          if (numberOfFinishedThreads == slaveArray.length) {
            round++;

            // if all slaves completed the round, master tells nodes to start next round
            sendRoundStartMsg();
            // Reset Done Count for next round messages
            numberOfFinishedThreads = 0;
          }
        } catch (Exception e) {
          e.printStackTrace();
        }

      }
      System.out.println(leaderSet.toString());
      System.out.println("Starting threads. ");
      startAllThreads(); // bug
      System.gc();
      printTree();
    } while (!masterMustDie);

    printTree();
    System.out.println("Master will now die. MaxUid " + coreLink + " round " + round);

  }

  /**
   * Print the nackAck set after the algorithm is done.
   */
  public void printTree() {
    sleep(10);
    System.out.println("\n\nPrinting the tree.");
    System.out.println("MaxId--------Parent <----- myId ---> branch edges (can overlap)");
    for (SlaveThread t : threadList) {
      System.out.print(t.coreLink + "---  " + t.getMyParent() + "<------" + t.getId() + "------>");
      for (Link i : t.getBranch()) {
        System.out.print(i.getTo() + " ");
      }
      System.out.println();
    }

    System.out.println("\n\nPrinting rejected.");
    System.out.println("MaxId--------Parent <----- myId ---> rejected edges (can overlap)");
    for (SlaveThread t : threadList) {
      System.out.print(t.coreLink + "---  " + t.getMyParent() + "<------" + t.getId() + "------>");
      for (Link i : t.getRejected()) {
        System.out.print(i.getTo() + " ");
      }
      System.out.println();
    }

    System.out.println("\n\nPrinting basic edges.");
    System.out.println("MaxId--------Parent <----- myId ---> basic edges (can overlap)");
    for (SlaveThread t : threadList) {
      System.out.print(t.coreLink + "---  " + t.getMyParent() + "<------" + t.getId() + "------>");
      for (Link i : t.getBasicLinks()) {
        System.out.print(i.getTo() + " ");
      }
      System.out.println();
    }
    sleep(10);
  }

  public void processRoundDoneMsg(Message m) {
    if (m.getParent() != -1) {
      leaderSet.remove(m.getSenderId());
    }
  }

  /**
   * Create all nodes/threads, set their names, neighbors, initiate the local copy
   * (two queues) of the global queue.
   */
  public void createThreads() {
    try {
      for (int row = 0; row < size; row++) {
        SlaveThread t = new SlaveThread(slaveArray[row], this, globalIdAndMsgQueueMap);
        leaderSet.add(slaveArray[row]);
        threadList.add(t);

        // add neighbors
        for (int col = 0; col < size; col++) {
          if (matrix[row][col] > 0) {
            // System.err.println(t.name + " " + slaveArray[row] + " " + slaveArray[col] + "
            // " + matrix[row][col]);
            t.insertNeighbour(new Link(slaveArray[row], slaveArray[col], matrix[row][col]));
          }
        }

        t.initLocalMessagesQueues();
      }

      // System.err.println("Created threads. ");
    } catch (Exception err) {
      err.printStackTrace();
    }
  }

  public void startAllThreads() {
    for (SlaveThread t : threadList) {
      t.run();
    }
  }

  public synchronized void sendRoundStartMsg() {
    for (int i : slaveArray) {
      // System.err.println("Send Round_Number msg to " + i);
      try {
        Message temp = new Message(id, i, 0, level, round, coreLink, "Round_Number");
        globalIdAndMsgQueueMap.get(i).put(temp);

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Debug only.
   */
  public void checkGlobalQueueNotEmpty() {
    for (Entry<Integer, LinkedBlockingQueue<Message>> p : globalIdAndMsgQueueMap.entrySet()) {
      System.out.println(p.getKey() + " " + p.getValue());
    }
  }

  /**
   * Set all slaves' terminate to true;
   */
  public void killAll() {
    for (SlaveThread s : threadList) {

      System.out.println("Terminated slave " + s.getId());
      try {
        s.terminated = true;

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
