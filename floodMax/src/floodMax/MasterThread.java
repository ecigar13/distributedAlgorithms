package floodMax;

import message.Message;
import sun.util.locale.provider.LocaleProviderAdapter;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.Queue;
import java.util.PrimitiveIterator.OfDouble;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.omg.CORBA.PUBLIC_MEMBER;

import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

import java.util.ArrayList;
import javafx.scene.chart.PieChart.Data;
import message.Message;
//import message.Message.MessageType;
import java.util.PriorityQueue;
import java.util.concurrent.*;

/**
 * MasterNode is a special case of a SlaveNode.
 * 
 * @author khoa, gunjan, sumit
 *
 */
public class MasterThread extends SlaveThread {
 ConcurrentHashMap<Integer, HashSet<Integer>> finalChildren;
  private static final int NULL = 0;
  protected int master_id = 0;
  // protected int sno_in_graph = 0;
  protected int master_round = 0;
  protected boolean newInfo = true;
  // protected MessageType status;
  // max id to be in sync with message class object. Not needed here. Junk value
  protected int max_uid = 0;;
  int parent;
  protected String MType;
  private LinkedBlockingQueue<Message> temp_priority_queue;
  private LinkedBlockingQueue<Message> t_priority_queue;
  private LinkedBlockingQueue<Message> temp_pq;
  private Message temp_Message_obj;
  protected int size;
  private int[] ids;
  protected int[][] matrix;
  // master will put information about the round in this hash map which is
  // accessible to all
  public ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> Data_Messages;
  public ConcurrentHashMap<Integer, Integer> Sno_id_mapping;
  // hash Map for storing children pointers
  public ConcurrentHashMap<Integer, ArrayList<Integer>> children = new ConcurrentHashMap<>();
  private Map<Integer, SlaveThread> slaves = new ConcurrentHashMap<Integer, SlaveThread>();
  private int Done_Count;
  private boolean not_found_leader;
  private int m_flag;
  protected ArrayList<Thread> threadList = new ArrayList<>();
  private int Leader;

  /**
   * Constructor
   * 
   * @param size
   * @param ids
   * @param matrix
   */
  public MasterThread(int size, int[] ids, int[][] matrix,
      ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> map) {
    this.size = size;
    this.ids = ids;
    this.matrix = matrix;
    this.Data_Messages = map;
    this.parent = NULL;
    // put master into concurrent hash map
    // temp_priority_queue = new LinkedBlockingQueue<>();
    Done_Count = 0;
    // Data_Messages = new ConcurrentHashMap<Integer,
    // LinkedBlockingQueue<Message>>();
    temp_pq = new LinkedBlockingQueue<>();
    // used for printing the tree at the end
    Sno_id_mapping = new ConcurrentHashMap<Integer, Integer>();
    this.not_found_leader = true;
    this.m_flag = 0;

    finalChildren = new ConcurrentHashMap<Integer, HashSet<Integer>>();

    // setNeighbors();
  }

  @Override
  public void run() {
    //System.out.println(" Inside run of master thread");
    Data_Messages.put(0, temp_pq);

    for (int i = 1; i < size; i++) {
      Sno_id_mapping.put(i, ids[i]);
    }

    // ExecutorService executor=
    // Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    try {
      for (int i = 1; i < size; i++) {
        Thread t = new Thread(new SlaveThread(ids[i], this, i, children, Sno_id_mapping));
        t.setName("Thread_" + i);
        t.start();
        // executor.execute(new SlaveThread(ids[i], this, i, children,Sno_id_mapping));
      }
    } catch (Exception err) {
      err.printStackTrace();
    }
    // executor.shutdown(); // once you are done with ExecutorService

    // put message generate by the leader into the rows of all the slaves

    for (int i = 1; i < size; i++) {
      Message msg = new Message(this.master_id, this.master_round, this.max_uid, "Round_Number");
      temp_priority_queue = new LinkedBlockingQueue<>();
      temp_priority_queue.add(msg);
//      System.out.println("Sending Round number message to " + i);
//      System.out.println("message for " + i + " is : " + temp_priority_queue);
      Data_Messages.put(i, temp_priority_queue);
    }

//	  for(int i = 0; i <size; i++)
//	  {
//		  System.out.println("Master: Size of threads is "+Data_Messages.get(i).size()+" Sno is "+ i);
//	  }

    while (not_found_leader) {

      temp_priority_queue = Data_Messages.get(0);
      while (temp_priority_queue.size() > 0) {
        //System.out.println("Master checking its queue: Size of master queue is " + temp_priority_queue.size());

        this.temp_Message_obj = temp_priority_queue.poll();
//			  System.out.println("Master checking its queue");
//        System.out.println("Size of master queue is " + temp_priority_queue.size());
//        System.out.println("message type to master " + this.temp_Message_obj.getmType() + "Master round number"
//            + this.master_round + "incoming msg round" + this.temp_Message_obj.getRound());

        if (temp_Message_obj.getmType().equals("Leader")) {
          try {
            Thread.sleep(1000);
          }
          
          
              catch(Exception e) {}
              
          Leader = temp_Message_obj.getUid();
          System.out.println("Leader Selected " + temp_Message_obj.getmaxUID());
          System.out.println("Tree formed is ");
          System.out.println(temp_Message_obj.getmaxUID());
          temp_priority_queue = new LinkedBlockingQueue<>();
          
          Message msg = new Message(this.master_id, this.master_round, this.max_uid, "Terminate");
          temp_priority_queue.add(msg);
          Data_Messages.put(temp_Message_obj.getSenderId(), temp_priority_queue);
          not_found_leader = false;
          m_flag = 1;
          break;
        }

        else if ((temp_Message_obj.getmType().equals("Done")) && (temp_Message_obj.getRound() == this.master_round)) {
          Done_Count++;
//          System.out.println("Master done count is " + Done_Count);
//          System.out.print("size - 1 is ");
//          System.out.println(size - 1);
          // all slaves completed the round
          if (Done_Count == size - 1) {

            Done_Count = 0;
            this.master_round++;
            //System.out.println("New round is***************************************************** " + this.master_round);

            for (int i = 1; i < size; i++) {
              Message msg = new Message(this.master_id, this.master_round, this.max_uid, "Round_Number");
              this.t_priority_queue = new LinkedBlockingQueue<>();
              this.t_priority_queue = Data_Messages.get(i);
              //System.out.println("Master Before addition Of round message " + i + " : " + t_priority_queue.size());
              this.t_priority_queue.add(msg);

              Data_Messages.put(i, this.t_priority_queue);
//              System.out.println("Master: updated hashmap for " + i);
//              System.out.println("Master: after addition :Size of common hash map for element : " + i + " : "
//                  + Data_Messages.get(i).size());

            }

          }

        }
        if (m_flag == 1) {

          break;
        }
      }
      printHashMapRecursive(Leader);
    }

  }

  public void printHashMapRecursive(int key) {
    // base
    if (finalChildren.get(key) == null) {
      return;
    }

    // recursion
   // System.out.println(key+" : ");
    for (Integer children : finalChildren.get(key)) {
      System.out.print(ids[children] + "  ");
    }
    System.out.println();
    
    HashSet<Integer> temp = finalChildren.get(key);
    finalChildren.remove(key);
    for (Integer j : temp) {
      printHashMapRecursive(j);
    }

  }
}

