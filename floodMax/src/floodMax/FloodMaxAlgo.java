package floodMax;
/*Project: Distributed Computing Project 1
Description: Leader election
Date: Septermber 26, 2018
Author: Gunjan Munjal
*/

import java.util.concurrent.BlockingQueue;
import floodMax.Message;
import java.util.concurrent.LinkedBlockingQueue;
import java.io.*;
import java.util.Scanner;

/*
Master thread creates other slave threads
 */
class Master extends Thread {
  Message msm;
  static int round = 0;

  private final BlockingQueue<Message> master_to_slave_bq;
  // private thread thread;
  private int Thread_UID;
  private int number_of_slaves;
  int[][] adj_graph = new int[number_of_slaves + 1][number_of_slaves + 1];

  Master(BlockingQueue<Message> q) {
    master_to_slave_bq = q;
  }

  Master(int UID, int Number_of_slaves, int adj_graph[][]) {
    Thread_UID = UID;
    System.out.println("creating thread " + Thread_UID);
    number_of_slaves = Number_of_slaves;

    master_to_slave_bq = new LinkedBlockingQueue<>();
    this.adj_graph = adj_graph;
  }

  public void run() {
    
    //should this be: create a number of threads, each has a unique uid
    //find diameter
    //then do while loop using the diameter condition?
    
    Master that = this;
    Thread thread = new Thread() {
      public void run() {
        for (int i = 1; i <= number_of_slaves; i++) {
          Slave sl = new Slave(i, that.adj_graph);
          sl.start();
        }
      }
    };
    thread.start();
    // wait for thread completion
    try {
      thread.join();
    } catch (Exception e) {
      e.printStackTrace();
    }
    // varibale to store diameter of the tree
    int diam = 0;

    Thread round_thread = new Thread() {
      int count = 0;
      boolean execution = true;

      public void run() {
        while (execution == true) {
          try {
            Message msg = master_to_slave_bq.take();
          } catch (Exception e) {
            e.printStackTrace();
          }

          count = count + 1;
          if (count == number_of_slaves) {
            round = round + 1;
            if (round < diam) {
              for (int i = 1; i <= number_of_slaves; i++) {
                msm = new Message();
                msm.setUid(i);
                msm.set_round(round);
                try {
                  master_to_slave_bq.put(msm);
                } catch (Exception e) {
                  e.printStackTrace();
                }
              }
            } else {
              execution = false;
            }
          }
        }
      }
    };
    round_thread.start();
  }

}

class Slave extends Thread {
  private final BlockingQueue<Message> master_to_slave_bq;
  private int thread_UID;

  Slave(int UID, int[][] adj_graph) {
    thread_UID = UID;
    System.out.println("creating slave thread " + thread_UID);
    master_to_slave_bq = new LinkedBlockingQueue<>();
  }

  public void run() {
    // code to cal diameter
  }
}

/*
 * 
 * FloodMax class to initialize the master thread Input:number of nodes in the
 * system
 * 
 */
public class FloodMaxAlgo {
  public static void main(String args[]) {
    
    // add read from file?
    System.out.println("Started execution");
    BlockingQueue<Message> bq = new LinkedBlockingQueue<>();
    Scanner sc = new Scanner(System.in);
    System.out.println("Please input number of nodes in the system");
    int nodes = sc.nextInt();
    // input graph
    int adj_graph[][] = {};
    new Master(0, nodes, adj_graph).start();
  }
}
