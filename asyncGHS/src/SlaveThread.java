
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.sun.xml.internal.ws.api.pipe.ThrowableContainerPropertySet;

import java.util.ArrayList;
import java.util.HashSet;

public class SlaveThread implements Runnable {
  protected String name;
  protected boolean isLeader = true;
  protected boolean terminated;

  protected int id;
  protected int round;
  private int myParent;
  protected double mwoe;
  protected MasterThread masterNode;
  protected int level = 0;
  boolean mwoeFound = false;

  protected Link coreLink = null;
  protected boolean waitingToConnect = false;
  protected Message currentSmallestReportMessage;
  protected Message currentSmallestTestMsg;

  protected ArrayList<Message> testMsgToRespond = new ArrayList<>();
  protected ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> globalIdAndMsgQueueMap;
  protected ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> localMsgToReduce;
  protected ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> localMsgToSend;
  protected LinkedBlockingQueue<Message> localMessageQueue = new LinkedBlockingQueue<>();

  protected ArrayList<Integer> sentConnect = new ArrayList<>();
  protected ArrayList<Integer> receivedConnect = new ArrayList<>();

  protected TreeSet<Integer> reportReceived = new TreeSet<>();
  protected ArrayList<Integer> waitingForResponse = new ArrayList<>();
  protected TreeSet<Link> basicEdge = new TreeSet<>(new CompareLinks());
  protected TreeSet<Link> branch = new TreeSet<>(new CompareLinks());
  protected TreeSet<Link> rejected = new TreeSet<>(new CompareLinks());

  protected Random r = new Random();

  public void printSlave() {
    System.out.printf("Name: %s \t isLeader %s \t level %s\n coreLink %s", name, isLeader, level, coreLink);
  }

  /**
   * 
   * The node with higher ID becomes parent. Move the Link to the right set.
   */
  public void merge(Message m) {
    System.out.printf("Merging %s and %s\n", id, m.getSenderId());
    if (id < m.getSenderId()) {
      myParent = m.getSenderId();
      isLeader = false;
    } else {
      isLeader = true;
    }

    // inefficient because using object.
    coreLink = m.getCore();
    branch.add(m.getCore());
    basicEdge.remove(m.getCore());

    level++;
    // broadcast change root to the rest of the tree, change their maxUid to this
    // maxUid.
  }

  /**
   * Local operation. Register child as absorbed. Send back "absorbed" message
   * with core and level.
   * 
   * @param m
   */
  public void parentAbsorb(Message m) throws InterruptedException {
    // clear all report because the core has changed.
    reportReceived.clear();
    currentSmallestReportMessage = null;
    currentSmallestTestMsg = null;

    for (Link l : basicEdge) {
      if (l.getTo() == m.getSenderId()) {
        basicEdge.remove(l);
        branch.add(l);
        break;
      }
    }
    localMsgToReduce.get(m.getSenderId())
        .put(new Message(id, m.getSenderId(), mwoe, level, r.nextInt(19) + 1, coreLink, "absorbed"));
  }

  /**
   * Use with "absorbed" msg. The child combine into higher level component. Set
   * core, level, parent and broadcast to all branches.
   * 
   * @param m
   *          message from higher level component.
   */
  public void childAbsorb(Message m) throws InterruptedException {
    // clear all report because the core has changed.
    isLeader = false;
    reportReceived.clear();
    currentSmallestReportMessage = null;
    currentSmallestTestMsg = null;

    waitingToConnect = false;

    for (Link l : basicEdge) {
      if (l.getTo() == m.getSenderId()) {
        basicEdge.remove(l);
        branch.add(l);
        myParent = m.getSenderId();
        coreLink = m.getCore();
        level = m.getLevel();
        break;
      }
    }
    broadcastAbsorb(m);

  }

  /**
   * If I'm the mwoe node (outside the component), check if I sent connect msg to
   * the sender. Then merge or absorb base on the level. If absorb, send
   * "absorbed" msg back.
   * 
   * If I'm the leaf node (path size is 1), then this is the changeRoot message. I
   * record that I sent connect message. Change my level and core to fit. Then
   * send it.
   * 
   * Else just relay it. Save level and core.
   * 
   */
  public void processConnectMessage(Message m) throws InterruptedException, NullPointerException {
    if (m.getPath().size() == 0) {
      // if I sent connect msg before. Then merge or absorb.
      receivedConnect.add(m.getSenderId());

      if (sentConnect.contains(m.getSenderId()) && receivedConnect.contains(m.getSenderId()) && level == m.getLevel()) {
        merge(m);
      } else if (sentConnect.contains(m.getSenderId()) && receivedConnect.contains(m.getSenderId())
          && level > m.getLevel()) {
        parentAbsorb(m);
      } // else, do nothing. Not enough condition to merge or absorb. See wikipedia.

    } else if (m.getPath().size() == 1) {
      // mType should be changeRoot from parent

      level = m.getLevel();
      coreLink = m.getCore();

      int temp = m.getPath().removeLast();
      sentConnect.add(temp);
      m.setSenderId(id);
      m.setReceiverId(temp);
      m.setmType("connect"); // in case this is changeRoot msg.
      localMsgToReduce.get(temp).put(m);
      waitingToConnect = true;
    } else {

      level = m.getLevel();
      coreLink = m.getCore();

      // I'm in the same component, keep sending it.
      int temp = m.getPath().removeLast();
      m.setSenderId(id);
      m.setReceiverId(temp);
      localMsgToReduce.get(temp).put(m);
    }

  }

  /**
   * Process messages int the queue.
   */
  public void processMessageTypes() throws InterruptedException {
    Message m;
    while (localMessageQueue.size() != 0) {
      m = localMessageQueue.poll();
      System.out.println(m);

      if (m.getmType().equals("Round_Number")) {
        processRoundNumber(m);// done
      } else if (m.getmType().equals("initiate")) {
        processInitMessage(m); // done
      } else if (m.getmType().equals("test")) {
        processTestMsg(m); // done
      } else if (m.getmType().equals("report")) {
        processReportMessage(m); // done
      } else if (m.getmType().equals("accept")) {
        processAcceptMessage(m); // done
      } else if (m.getmType().equals("reject")) {
        processRejectMsg(m); // done
      } else if (m.getmType().equals("changeRoot")) {
        // changeRoot is essentially connect msg
        processConnectMessage(m); // done
      } else if (m.getmType().equals("connect")) {
        processConnectMessage(m); // done
      } else if (m.getmType().equals("levelMismatched")) {
        processLevelMismatchedMsg(m);
      } else if (m.getmType().equals("absorbed")) {
        childAbsorb(m);
      }
    }
  }

  /**
   * Need to implement this. Don't use while loop.
   */
  public void run() {
    printSlave();
    if (!terminated) {
      try {
        if (coreLink == null) {
          sendTestToSmallestBasic();
        }
        fetchFromGlobalQueue();
        // if I'm leader, send initiate.
        processMessageTypes();
        respondToTestMsg();
        decideToSendReportMsg();
        // send msg I need to send into local queue.
        reduceRoundInMsg();
        drainToGlobalQueue();
        sendRoundDoneToMaster();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

  }

  /**
   * Only do this if I got all reports from all children and accept msg from
   * basic.
   * 
   * @throws InterruptedException
   */
  public void decideToSendReportMsg() throws InterruptedException {
    if (reportReceived.size() == branch.size() && currentSmallestReportMessage != null
        && currentSmallestTestMsg != null) {

      // If I got report from all branches, Compare it with what I get from reports.
      Message msgToUse;
      if (currentSmallestReportMessage.getMwoe() > currentSmallestTestMsg.getMwoe()) {
        msgToUse = currentSmallestTestMsg;
      } else {
        msgToUse = currentSmallestReportMessage;
      }

      if (isLeader) {
        // if leader, process the message and broadcast back.
        // what if my mwoe is in leader's basic edges???
        broadcastConnect(msgToUse);
      } else {
        // if I pick mwoe from basic edge, construct new msg and send up.
        // also add the last node.

        msgToUse.setSenderId(id);
        msgToUse.getPath().add(id);
        localMsgToReduce.get(myParent).put(msgToUse);
      }

      // clear my set of report and set mwoe to maximum.
      reportReceived.clear();
      currentSmallestReportMessage = null;
      currentSmallestTestMsg = null;
      mwoe = Double.MAX_VALUE;
    }

  }

  /**
   * Only run this method if leader got report msg from all its children.
   * 
   * If path is 1, then I'm the leaf node. Record that I sent connect to the mwoe
   * node and send it.
   * 
   * if currentReportMsg is not null then the mwoe belongs to my children. Send
   * the changeRoot message back down the path. This is changeRoot message. Near
   * the end of the path, this changeRoot becomes connect message.
   * 
   * @param m
   *          message to be used.
   * @param id
   * @throws InterruptedException
   */
  public void broadcastConnect(Message m) throws InterruptedException {
    // reuse the message.
    m.setLevel(level);
    m.setRound(r.nextInt(19) + 1);
    m.setmType("changeRoot");

    // save behavior as processConnectMessage()
    processConnectMessage(m);

  }

  /**
   * Process Test messages. If my lvl is > your lvl.
   * 
   * "accept" message will be used to construct report msg, so always add the
   * path.
   * 
   * If my lvl < your lvl, maybe I'm in the same component but I don't know yet.
   * 
   * @param m
   */
  public void processTestMsg(Message m) throws InterruptedException {
    if (level >= m.getLevel()) {
      if (coreLink.equals(m.getCore())) {
        localMsgToReduce.get(m.getSenderId())
            .put(new Message(id, m.getSenderId(), m.getMwoe(), level, r.nextInt(19) + 1, coreLink, "reject"));

      } else if (coreLink != m.getCore()) {
        Message temp = new Message(id, m.getSenderId(), m.getMwoe(), level, r.nextInt(19) + 1, coreLink, "accept");
        temp.getPath().add(id); // important step in deciding to send report message.
        localMsgToReduce.get(m.getSenderId()).put(temp);
      }
    } else { // can't decide, wait until level is high enough to respond. See wikipedia algo.
      testMsgToRespond.add(m);
    }
  }

  /**
   * Go through the testMsgToRespond array and check with my level. Respond if my
   * lvl is >= msg level.
   * 
   */
  public void respondToTestMsg() throws InterruptedException {
    for (Message m : testMsgToRespond) {
      if (level >= m.getLevel()) {
        processTestMsg(m);
        testMsgToRespond.remove(m);
      }
    }
  }

  /**
   * Send initiate to all branches except parent. Trigger find mwoe.
   */
  public void sendInitiateToBranch() throws InterruptedException {
    for (Link e : branch) {
      if (e.getFrom() == myParent) {
        continue;
      }
      if (!waitingForResponse.contains(e.getTo())) {
        System.out.println("Send initiate to " + e.getTo());
        localMsgToReduce.get(e.getTo())
            .put(new Message(id, e.getTo(), e.getWeight(), level, r.nextInt(19) + 1, coreLink, "initiate"));
        waitingForResponse.add(e.getTo());
      }
    }
  }

  /**
   * Send test message to the smallest edge in basic edges. Don't wait for
   * response because bookkeeping for test msg is too much.
   * 
   * @throws InterruptedException
   */
  public void sendTestToSmallestBasic() throws InterruptedException {
    Link e = basicEdge.first();
    if (!waitingForResponse.contains(e.getTo())) {
      System.out.println("Send test to " + e.getTo());
      localMsgToReduce.get(e.getTo())
          .put(new Message(id, e.getTo(), e.getWeight(), r.nextInt(19) + 1, round, coreLink, "test"));
      waitingForResponse.add(e.getTo());

    } else {
      System.out.println("Waiting for response from " + e.getTo());
    }
  }

  /**
   * Record the msg as received. Save the report msg if it has smaller mwoe than
   * previious report msg. Otherwise, discard.
   * 
   * @param m
   */
  public void processReportMessage(Message m) throws InterruptedException {
    // save the msg with smallest mwoe. Discard the rest.
    reportReceived.add(m.getSenderId());
    if (currentSmallestReportMessage == null || currentSmallestReportMessage.getMwoe() > m.getMwoe()) {
      currentSmallestReportMessage = m;
    }
  }

  /**
   * Print out the hashmap/queue to debug.
   * 
   * @param hm
   */
  public synchronized void printMessagesToSendMap(ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> hm) {
    for (Entry<Integer, LinkedBlockingQueue<Message>> e : hm.entrySet()) {
      System.err.println(e.getValue().size() + " " + e.getKey() + " " + e.getValue());
    }
    sleep();
  }

  public void insertNeighbour(Link l) {
    this.basicEdge.add(l);
  }

  public int getId() {
    return id;
  }

  public void sleep() {
    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getMyParent() {
    return myParent;
  }

  public TreeSet<Link> getBasicLinks() {
    return basicEdge;
  }

  /**
   * Go through messages in my queue and reduce the round by 1. Not less than 0.
   * Move all msg that are 0 at the front to the localMsgToSend queue.
   * 
   * Maybe implement map reduce.
   */
  public void reduceRoundInMsg() throws InterruptedException {

    for (Entry<Integer, LinkedBlockingQueue<Message>> e : localMsgToReduce.entrySet()) {
      for (Message message : e.getValue()) {
        int temp = message.getRound() - 1 >= 0 ? message.getRound() - 1 : 0;
        message.setRound(temp);
      }

      if (e.getValue().size() != 0) {
        while (e.getValue().peek().getRound() <= 0) {
          localMsgToSend.get(e.getKey()).put(e.getValue().remove());
        }

      }

    }
  }

  /**
   * Drain the local msg queue and put all in the global queue. Call this at the
   * end of each round. The global queue's structure is similar to this.
   */
  public synchronized void drainToGlobalQueue() {

    for (Entry<Integer, LinkedBlockingQueue<Message>> e : localMsgToSend.entrySet()) {
      e.getValue().drainTo(globalIdAndMsgQueueMap.get(e.getKey()));

      if (!e.getValue().isEmpty()) {
        System.err.println("Queue is not empty at end of round");
      }

    }
  }

  /**
   * Initiate the local queues. Otherwise it will throw nullPointerException.
   */
  public void initLocalMessagesQueues() {
    localMsgToReduce.put(masterNode.getId(), new LinkedBlockingQueue<Message>());
    for (Link l : basicEdge) {
      localMsgToReduce.put(l.getTo(), new LinkedBlockingQueue<Message>());
    }

    localMsgToSend.put(masterNode.getId(), new LinkedBlockingQueue<Message>());
    for (Link l : basicEdge) {
      localMsgToSend.put(l.getTo(), new LinkedBlockingQueue<Message>());
    }
    System.err.println("Done initiating SlaveThread queues.");
  }

  /**
   * Put round done message to local queue.
   */
  public void sendRoundDoneToMaster() {
    try {
      localMsgToSend.get(masterNode.getId())
          .put(new Message(id, masterNode.getId(), mwoe, level, round, coreLink, "Done"));

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Drain the global queue to this local queue. The Global queue will have zero
   * element.
   * 
   * @return
   */
  public void fetchFromGlobalQueue() {
    if (!globalIdAndMsgQueueMap.get(id).isEmpty()) {
      globalIdAndMsgQueueMap.get(id).drainTo(localMessageQueue);
    }

    if (!globalIdAndMsgQueueMap.get(id).isEmpty()) {
      System.err.println("Global queue is not empty. We have a prolem.");
    }
  }

  /**
   * set my round to the round master tells me. Will not confuse with delayRound
   * in message.
   * 
   * @param m
   */
  public void processRoundNumber(Message m) {
    round = m.getRound();
  }

  /**
   * Send initiate to all branches and send test to smallest basic edge.
   * 
   * @param m
   */
  public void processInitMessage(Message m) throws InterruptedException {
    sendTestToSmallestBasic();
    sendInitiateToBranch();
  }

  /**
   * Add the edge to rejected set. Remove from basic edge.Send test msg to the
   * smallest basic edge.
   * 
   * @param m
   */
  public void processRejectMsg(Message m) throws InterruptedException {
    for (Link l : basicEdge) {
      if (l.getTo() == m.getSenderId()) {
        rejected.add(l);
        basicEdge.remove(l);

      }
    }
    sendTestToSmallestBasic();
  }

  /**
   * Remove the sender from waiting for response (because I got the response).
   * Save the test msg.
   * 
   * @param m
   */
  public void processAcceptMessage(Message m) throws InterruptedException {
    waitingForResponse.remove(m.getSenderId());
    currentSmallestTestMsg = m;
  }

  /**
   * If level is mismatched, send test to the next one.
   * 
   * @param m
   * @throws InterruptedException
   */
  public void processLevelMismatchedMsg(Message m) throws InterruptedException {
    for (Link l : basicEdge) {
      if (l.getWeight() > m.getMwoe()) {
        sendTestToSmallestBasic();
      }
    }
  }

  /**
   * Broadcast absorb to all branches except parents.
   * 
   * @param m
   *          "absorbed" message received.
   * @throws InterruptedException
   */
  public void broadcastAbsorb(Message m) throws InterruptedException {
    if (branch.isEmpty()) {
      return;
    }

    for (Link l : branch) {
      if (l.getTo() == myParent) {
        continue;
      }
      // broadcast absorbed msg to all branches.
      localMsgToReduce.get(l.getTo())
          .put(new Message(id, l.getTo(), mwoe, level, r.nextInt(19) + 1, coreLink, "absorbed"));
    }
  }

  public SlaveThread() {
  }

  /**
   * Constructor.
   * 
   * @param id
   * @param masterNode
   */

  public SlaveThread(int id, MasterThread masterNode,
      ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> globalIdAndMsgQueueMap) {
    this.id = id;
    this.coreLink = null;
    this.myParent = -1;
    this.mwoe = Double.MAX_VALUE;
    this.masterNode = masterNode;
    this.round = 0;
    this.terminated = false;

    name = "Thread_" + id;

    this.globalIdAndMsgQueueMap = globalIdAndMsgQueueMap;
    this.localMsgToReduce = new ConcurrentHashMap<>();
    this.localMsgToSend = new ConcurrentHashMap<>();
    // init local messages to send will be done after construction in MasterThread
    // because Links are added after construction.
  }

}