package floodMax;

import message.*;
import java.util.Set;
import java.util.Queue;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.omg.CORBA.PUBLIC_MEMBER;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Iterator;
import com.apple.eawt.AppEvent.ScreenSleepEvent;
import com.sun.glass.ui.TouchInputSupport;
import com.sun.javafx.animation.TickCalculation;
import com.sun.jmx.snmp.SnmpStringFixed;
import com.sun.org.apache.bcel.internal.generic.GETFIELD;
import com.sun.org.apache.xml.internal.dtm.ref.DTMDefaultBaseIterators.ChildrenIterator;
import com.sun.org.apache.xml.internal.security.keys.keyresolver.implementations.PrivateKeyResolver;
import com.sun.org.apache.xml.internal.serializer.utils.Messages;
import com.sun.prism.PhongMaterial.MapType;

import message.*;
import java.lang.Exception;

public class SlaveThread implements Runnable {

	protected int id;
	private int sno_in_graph;
	private MasterThread masterNode;
	// private SlaveThread parent;
	private int parent;

	public int getParent() {
		return parent;
	}

	int max_uid;
	ArrayList<Integer> neighbours;
	protected boolean flag_0; // check if round is 0 or not
	protected String Mtype;
	protected int round;
	Message temp_message_var;
	private int NACK_Count;
	private int ACK_Count;
	public ConcurrentHashMap<Integer, ArrayList<Integer>> slave_children;
	private LinkedBlockingQueue<Message> temp_priority_queue;
	private LinkedBlockingQueue<Message> temp_pq;
	private ArrayList<Integer> list_of_children;
	static boolean terminate;
	private LinkedBlockingQueue<Message> temp_msg_pbq;
	protected Map<Integer, Integer> distance = new ConcurrentHashMap<Integer, Integer>();
	protected Map<Integer, SlaveThread> neighbors;
	// protected Queue<Message> nextRoundMsg = new LinkedBlockingQueue<Message>();
	// protected Queue<Message> thisRoundMsg = new LinkedBlockingQueue<Message>();
	private LinkedBlockingQueue<Messages_in_queue> msgs_in_queues;
	private Messages_in_queue temp_obj;
	private Queue<Integer> final_output;
	// Empty constructor for subclass.
	private int current_node;
	private static ConcurrentHashMap<Integer, Integer> Sno_id_mapping;
	private int Processed_Messages;
	private int flag;
	private int no_of_msg_to_process;

	public SlaveThread() {
	}

	/**
	 * Constructor.
	 * 
	 * @param id
	 * @param masterNode
	 * @param sno        = row in the matrix
	 */

	public SlaveThread(int id, MasterThread masterNode, int sno,
			ConcurrentHashMap<Integer, ArrayList<Integer>> children,
			ConcurrentHashMap<Integer, Integer> Sno_id_mapping) {
		this.id = id;
		this.max_uid = id;
		this.masterNode = masterNode;
		this.sno_in_graph = sno;
		temp_pq = new LinkedBlockingQueue<>();
		// temp_priority_queue = new LinkedBlockingQueue<>();
		// this.newInfo = true;
		neighbours = new ArrayList<>();
		this.round = 0;
		this.NACK_Count = 0;
		this.ACK_Count = 0;
		this.slave_children = children;
		slave_children = new ConcurrentHashMap<>();
		list_of_children = new ArrayList<>();
		// temp_msg_pbq = new LinkedBlockingQueue<Message>();
		msgs_in_queues = new LinkedBlockingQueue<>();
		final_output = new LinkedList<Integer>();
		this.Sno_id_mapping = Sno_id_mapping;
		this.terminate = false;
		neighbors = new ConcurrentHashMap<Integer, SlaveThread>();
		this.parent = this.sno_in_graph;
		flag = 0;
		// find neighbours
		for (int temp = 1; temp < masterNode.size; temp++) {

			// if edge exist in the graph provided
			if (masterNode.matrix[this.sno_in_graph][temp] == 1) {
				neighbours.add(temp);
			}
		}
		masterNode.Data_Messages.put(this.sno_in_graph, temp_pq);
		masterNode.finalChildren.put(this.sno_in_graph, new HashSet<Integer>());

	}

	public synchronized void run() {

		while (!terminate) {

			this.temp_priority_queue = masterNode.Data_Messages.get(this.sno_in_graph);

			// newInfo = false;
			while (!this.temp_priority_queue.isEmpty()) {

				this.temp_message_var = this.temp_priority_queue.poll();

				System.out.println("Size of queue is when message from master " + this.temp_priority_queue.size()
						+ "Sno thread is " + this.sno_in_graph + "Message type is " + this.temp_message_var.getmType()
						+ " from: " + this.temp_message_var.getSenderId());

				System.out.println();

				if (this.temp_message_var.getmType().equals("Terminate")) {
//				  		System.out.println("Inside terminate for slave thread");
//				  		Set entrySet = Sno_id_mapping.entrySet();
//			  		    // Obtaining an iterator for the entry set
//			  		    Iterator<Map.Entry<Integer, Integer>> it = Sno_id_mapping.entrySet().iterator();
//			  		    
//				  		//output the graph and stop execution
//				  		current_node = this.id;
//				  		final_output.add(current_node);
//				  		System.out.print("The tree formed is " + current_node +"-->");
//				  		while(!(final_output.isEmpty()))
//				  		{
//				  			current_node = final_output.poll();
//				  			
//				  		    // Iterate through HashMap entries(Key-Value pairs)
//				  		    while(it.hasNext())
//				  		    {
//				  		    
//				  		       Map.Entry<Integer, Integer> m_e = it.next();
//				  		       int val = m_e.getValue();
//				  		       if(val == current_node)
//				  		       {
//				  		    	 this.list_of_children = this.slave_children.get(m_e.getKey()); 
//				  		       }
//				  		   }
//				  			while(!(this.list_of_children.isEmpty()))
//				  			{
//				  				final_output.add(this.list_of_children.get(0));
//				  				this.list_of_children.remove(0);
//				  			}
//				  			System.out.print(current_node +"-->");
//				  		}
				  		terminate = true;
					System.out.println("Stopping execution");
					break;
				}

				else if (this.temp_message_var.getmType().equals("Round_Number")) {

					temp_priority_queue = new LinkedBlockingQueue<>();
					this.Processed_Messages = 0;
					this.round = temp_message_var.getRound();
					System.out.println(
							"Inside else if: Round Number is " + this.round + " Sno thread is " + this.sno_in_graph);

					if (this.round == 0) {
						// send explore message to all the neighbors
						System.out.println("Round is 0");
						// msgs_in_queues.removeAll(msgs_in_queues);
						int neighbour_id;
						System.out.println("Size of neighbour list is " + this.neighbours.size() + "Sno thread is "
								+ this.sno_in_graph);
						for (int i = 0; i < this.neighbours.size(); i++) {
							neighbour_id = this.neighbours.get(i);
							System.out.println(this.sno_in_graph + " sno " + "neighbour id is " + neighbour_id
									+ ", parent is " + this.parent);
							System.out.println();
							if (neighbour_id != this.parent) {
								System.out.println("Round 0: Sending explore message to neighbor " + neighbour_id
										+ " from " + this.sno_in_graph);
								Message temp_msg = new Message(this.sno_in_graph, this.round + 1, this.max_uid,
										"Explore");
								temp_obj = new Messages_in_queue(neighbour_id, temp_msg);
								// temp_obj.node_s_no = neighbour_id;
								// temp_obj.msg_to_be_sent = temp_msg;
								this.msgs_in_queues.add(temp_obj);
								// msg_to_be_sent.append(neighbour_id+":"+temp_msg+";");

							}
						}
						// tell Master round complete
						System.out.println(
								"send round " + this.round + " done message to master by thread " + this.sno_in_graph);
						Message Master_message = new Message(this.id, this.round, this.max_uid, "Done");
						temp_msg_pbq = new LinkedBlockingQueue<Message>();
						temp_msg_pbq = masterNode.Data_Messages.get(0);
						temp_msg_pbq.add(Master_message);
						masterNode.Data_Messages.put(0, temp_msg_pbq);
						try {
							Thread.sleep(1000);
						} catch (Exception e) {
						}

					}

					else {
						// send messages intended for this round
						this.flag = 0;
						System.out.println("Round > 0: checking local queue for message sending ;sno"
								+ this.sno_in_graph + "Message ques size is" + this.msgs_in_queues.size());
						System.out.println(this.sno_in_graph + " --------->>>>flag value is " + this.flag);
						System.out.println(this.sno_in_graph + " data message queue size is"
								+ masterNode.Data_Messages.get(this.sno_in_graph).size());
						System.out.println("Put data in the common hashmap");
						Messages_in_queue transit_message = new Messages_in_queue();
						while (!this.msgs_in_queues.isEmpty()) {

							transit_message = this.msgs_in_queues.poll();
							// System.out.println("before:::sender Sno"+ this.sno_in_graph+"Get id :
							// "+transit_message.GetId()+"Get msg: "+transit_message.GetMsg());
							System.out.println("before:::sender Sno" + this.sno_in_graph + "Get id : "
									+ transit_message.GetId() + "Get msg: " + transit_message.GetMsg());

							temp_msg_pbq = new LinkedBlockingQueue<Message>();
							temp_msg_pbq = this.masterNode.Data_Messages.get(transit_message.GetId());
							temp_msg_pbq.add(transit_message.GetMsg());
							this.masterNode.Data_Messages.put(transit_message.GetId(), temp_msg_pbq);
						}
						try {
							Thread.sleep(5000);
						} catch (Exception e) {
						}

//			  						for (int i = 0 ; i < this.msgs_in_queues.size(); i++)
//			  						{
//			  							this.msgs_in_queues.remove(i);
//			  						}

						this.temp_priority_queue = masterNode.Data_Messages.get(this.sno_in_graph);
						// should not give error; size never 0

						System.out.println(this.sno_in_graph + " : messages from neighbors if any "
								+ this.temp_priority_queue.size());
						this.no_of_msg_to_process = this.temp_priority_queue.size();
						// System.out.println(this.sno_in_graph+" number of messgaes in
						// temp_priority_queue:::Number of messgaes to process
						// "+this.no_of_msg_to_process);
						while (!(this.temp_priority_queue.isEmpty())) {

							this.temp_message_var = this.temp_priority_queue.poll();

							System.out.println("Id is " + this.sno_in_graph + " message type is "
									+ this.temp_message_var.getmType() + " from " + this.temp_message_var.getSenderId()
									+ "Round is " + this.round + "message round is "
									+ this.temp_message_var.getRound());
							if (this.temp_message_var.getRound() == this.round) {

								// increments the round number
								if (this.temp_message_var.getmType().equals("Explore")) {
									System.out.println("****** Inside explore *********");
									// System.out.println("Current thread max uid"+this.max_uid+"Incoming thread max
									// uid"+temp_message_var.getmaxUID());
									this.Processed_Messages++;

									if (this.max_uid < temp_message_var.getmaxUID()) {
										System.out.println();
										System.out.println("Current thread max uid" + this.max_uid
												+ "Incoming thread max uid" + temp_message_var.getmaxUID());
										this.max_uid = temp_message_var.getmaxUID();
										this.parent = temp_message_var.getSenderId();
										System.out.println("$$$$$$$$$ Sno is " + this.sno_in_graph + " max id is "
												+ this.max_uid + "Parent is " + this.parent);
										this.flag = 1;
									} else if (this.max_uid == temp_message_var.getmaxUID()) {
										System.out.println();
										System.out.println("Current thread max uid" + this.max_uid
												+ "Incoming thread max uid" + temp_message_var.getmaxUID());
										// check which parent node has bigger id and choose a parent
//									  if(this.id > temp_message_var.getSenderId())//take form uid hash map and not parent
//									  {
										System.out.println("this.max_uid " + this.max_uid
												+ " temp_message_var.getmaxUID()" + temp_message_var.getmaxUID());
										System.out.println("Sending Nack message from  " + this.sno_in_graph + " to "
												+ temp_message_var.getSenderId());
										Message temp_msg = new Message(this.sno_in_graph, this.round + 1, this.max_uid,
												"N_ACK");
										temp_obj = new Messages_in_queue(temp_message_var.getSenderId(), temp_msg);
										// temp_obj.node_s_no = temp_message_var.getSenderId();
										// temp_obj.msg_to_be_sent = temp_msg;
										msgs_in_queues.add(temp_obj);
										System.out.println("$$$$$$$$$ Sno is " + this.sno_in_graph + " max id is "
												+ this.max_uid + "Parent is " + this.parent);
//									  }
//									  else
//									
//									  {
//										  System.out.println();
//										  System.out.println("this.max_uid "+ this.max_uid+" temp_message_var.getmaxUID()"+temp_message_var.getmaxUID());
//										  System.out.println("Sending nack message from  "+this.sno_in_graph+" to "+this.parent);
//										  Message temp_msg= new Message(this.sno_in_graph,this.round+1,this.max_uid,"N_ACK"); 
//										  temp_obj = new Messages_in_queue(this.parent,temp_msg);
//										  //temp_obj.node_s_no = this.parent;
//										  //temp_obj.msg_to_be_sent = temp_msg;
//										  msgs_in_queues.add(temp_obj);
//										
//										  this.parent = temp_message_var.getSenderId();  
//										  System.out.println("$$$$$$$$$ Sno is "+this.sno_in_graph+" max id is " +this.max_uid+"Parent is "+this.parent);
//									  }

										this.flag = 0;
									} else if (this.max_uid > temp_message_var.getmaxUID()) {
										if ((this.sno_in_graph != this.parent)) {
											System.out.println();
											System.out.println("Current thread max uid" + this.max_uid
													+ "Incoming thread max uid" + temp_message_var.getmaxUID());
											System.out.println("Sending dummy message from  " + this.sno_in_graph
													+ " to " + this.parent);
											Message temp_msg = new Message(this.sno_in_graph, this.round + 1,
													this.max_uid, "DUMMY");
											temp_obj = new Messages_in_queue(temp_message_var.getSenderId(), temp_msg);
											// temp_obj.node_s_no = this.parent;
											// temp_obj.msg_to_be_sent = temp_msg;
											msgs_in_queues.add(temp_obj);
											System.out.println("$$$$$$$$$ Sno is " + this.sno_in_graph + " max id is "
													+ this.max_uid + "Parent is " + this.parent);
										}
										this.flag = 0;
									}
								} else if (temp_message_var.getmType().equals("N_ACK")) {
									this.Processed_Messages++;
									System.out.println();
									System.out.println("Inside Nack");
									this.NACK_Count++;
									System.out.println(
											this.sno_in_graph + "-->N_Ack count is :::::::::: " + this.NACK_Count);

									if ((NACK_Count == neighbours.size() - 1) && (this.parent != this.sno_in_graph)) {
										System.out.println();
										System.out.println("Sending ack message from  " + this.sno_in_graph + " to "
												+ this.parent);
										Message temp_msg = new Message(this.sno_in_graph, this.round + 1, this.max_uid,
												"ACK");
										temp_obj = new Messages_in_queue(this.parent, temp_msg);
										// temp_obj.node_s_no = this.parent;
										// temp_obj.msg_to_be_sent = temp_msg;
										msgs_in_queues.add(temp_obj);
									}
									// for leader
//							  		else if(ACK_Count == neighbours.size() )
//							  		{
//							  			System.out.println("Sending Leader message to master by "+this.sno_in_graph);
//							  			Message temp_msg= new Message(this.sno_in_graph,this.round+1,this.max_uid,"Leader"); 
//							  			temp_obj = new Messages_in_queue(0,temp_msg);
//	//									  temp_obj.node_s_no = 0;
//	//									  temp_obj.msg_to_be_sent = temp_msg;
//										  msgs_in_queues.add(temp_obj);
//							  			
//							  		}
									// for internal nodes
									else if ((NACK_Count + ACK_Count == neighbours.size() - 1)
											&& (this.parent != this.sno_in_graph)) {
										System.out.println();
										System.out.println(this.sno_in_graph + "-->Sending ack message from  "
												+ this.sno_in_graph + " to " + this.parent);
										Message temp_msg = new Message(this.sno_in_graph, this.round + 1, this.max_uid,
												"ACK");
										temp_obj = new Messages_in_queue(this.parent, temp_msg);
										// temp_obj.node_s_no = this.parent;
										// temp_obj.msg_to_be_sent = temp_msg;
										msgs_in_queues.add(temp_obj);
									}

								} else if (temp_message_var.getmType().equals("ACK")) {
								  HashSet<Integer> children=new HashSet<>();
								  children=masterNode.finalChildren.get(this.sno_in_graph);
								  children.add(temp_message_var.getSenderId());
								  masterNode.finalChildren.put(this.sno_in_graph, children);
									this.Processed_Messages++;
									System.out.println();
									System.out.println("Inside ack");
									this.ACK_Count++;
									System.out.println(this.sno_in_graph + "-->Ack count is :::::::::: " + this.ACK_Count+"Child is "+this.temp_message_var.getSenderId());
									
									    list_of_children.add(temp_message_var.getSenderId());
									
									// add children names to the hash map
									slave_children.put(this.sno_in_graph, list_of_children);

//								  if((NACK_Count == neighbours.size() - 1)&&(this.parent != this.sno_in_graph))
//							  		{
//							  			System.out.println("Sending ack message from  "+this.sno_in_graph+" to "+this.parent);
//							  			  Message temp_msg= new Message(this.sno_in_graph,this.round+1,this.max_uid,"ACK"); 
//							  			  temp_obj = new Messages_in_queue(this.parent,temp_msg);
//	//									  temp_obj.node_s_no = this.parent;
//	//									  temp_obj.msg_to_be_sent = temp_msg;
//										  msgs_in_queues.add(temp_obj);
//							  		}
									// for leader
									if (ACK_Count == neighbours.size()) {
										System.out.println();
										System.out.println("Sending Leader message to master by " + this.sno_in_graph);
										// Message temp_msg= new
										// Message(this.sno_in_graph,this.round+1,this.max_uid,"Leader");
										// temp_obj = new Messages_in_queue(0,temp_msg);
										// temp_obj.node_s_no = 0;
										// temp_obj.msg_to_be_sent = temp_msg;
										// msgs_in_queues.add(temp_obj);

										System.out.println();
										// System.out.println("send round "+this.round +" done message to master by
										// thread "+this.sno_in_graph);
										Message Master_message = new Message(this.sno_in_graph, this.round + 1,
												this.max_uid, "Leader");
										temp_msg_pbq = new LinkedBlockingQueue<Message>();
										// temp_msg_pbq = masterNode.Data_Messages.get(0);
										temp_msg_pbq.add(Master_message);
										masterNode.Data_Messages.put(0, temp_msg_pbq);

									}
									// for internal nodes
									else if ((NACK_Count + ACK_Count == neighbours.size() - 1)
											&& (this.parent != this.sno_in_graph)) {
										System.out.println();
										System.out.println("Sending ack message from  " + this.sno_in_graph + " to "
												+ this.parent);
										Message temp_msg = new Message(this.sno_in_graph, this.round + 1, this.max_uid,
												"ACK");
										temp_obj = new Messages_in_queue(this.parent, temp_msg);
										// temp_obj.node_s_no = this.parent;
										// temp_obj.msg_to_be_sent = temp_msg;
										msgs_in_queues.add(temp_obj);
									}

								} else if (this.temp_message_var.getmType().equals("DUMMY")) {
									System.out.println();
									System.out.println(this.sno_in_graph + " is processing dummy message from "
											+ this.temp_message_var.getSenderId());
									this.Processed_Messages++;
								}
							}

							if (flag == 1) {
								msgs_in_queues = new LinkedBlockingQueue<>();
								for (int i = 0; i < this.neighbours.size(); i++) {
									int neighbour_id;
									neighbour_id = this.neighbours.get(i);
									System.out.println();
									System.out.println("Inside flag == 1");
									System.out.println(this.sno_in_graph + " sno neighbour id is " + neighbour_id
											+ ", parent is " + this.parent);
									System.out.println();
									if (neighbour_id != this.parent) {
										System.out.println(
												this.msgs_in_queues.size() + " -  Sending explore message to neighbor "
														+ neighbour_id + " from " + this.sno_in_graph);
										Message temp_msg = new Message(this.sno_in_graph, this.round + 1, this.max_uid,
												"Explore");
										temp_obj = new Messages_in_queue(neighbour_id, temp_msg);
										// temp_obj.node_s_no = neighbour_id;
										// temp_obj.msg_to_be_sent = temp_msg;
										this.msgs_in_queues.add(temp_obj);
										// msg_to_be_sent.append(neighbour_id+":"+temp_msg+";");

									}
								}
								if (this.msgs_in_queues.isEmpty()) {
									System.out.println();
									System.out.println("No one to send message to ");
									this.NACK_Count = this.neighbours.size();
									System.out.println("Sending ack message from  " + this.sno_in_graph + " to " + this.parent);
									Message temp_msg = new Message(this.sno_in_graph, this.round + 1, this.max_uid,"ACK");
									temp_obj = new Messages_in_queue(this.parent, temp_msg);
									this.msgs_in_queues.add(temp_obj);
								}

							}
						}
						System.out.println(this.sno_in_graph
								+ " After ::::::::number of messgaes in temp_priority_queue:::Number of messgaes to process "
								+ this.no_of_msg_to_process);

						System.out.println("Sno is " + this.sno_in_graph + "Processed Message is " + this.Processed_Messages);
						if ((this.Processed_Messages == this.neighbours.size()) || (this.Processed_Messages == this.no_of_msg_to_process)) {
							System.out.println();
							System.out.println("send round " + this.round + " done message to master by thread "
									+ this.sno_in_graph);
							Message Master_message = new Message(this.id, this.round, this.max_uid, "Done");
							temp_msg_pbq = new LinkedBlockingQueue<Message>();
							temp_msg_pbq = masterNode.Data_Messages.get(0);
							temp_msg_pbq.add(Master_message);
							masterNode.Data_Messages.put(0, temp_msg_pbq);

							try {
								Thread.sleep(10000);
							} catch (Exception e) {
							}

						}
					}
				}
			}
		}
	}

	class Messages_in_queue {
		int node_s_no;
		Message msg_to_be_sent;

		public Messages_in_queue() {
		}

		public Messages_in_queue(int sno, Message msg) {
			this.node_s_no = sno;
			this.msg_to_be_sent = msg;
		}

		public int GetId() {
			return node_s_no;
		}

		public Message GetMsg() {
			return msg_to_be_sent;
		}

	};
	
	

}

// after done processing all the messages, send messages for next round
// send explore messages to all neighbors except parent
//			  	if (flag_0)
//			  	{
//			  		System.out.println("Inside new info if true");
//			  		msgs_in_queues.removeAll(msgs_in_queues);
//			  		int neighbour_id;
//			  		System.out.println("Size of neighbour list is "+neighbours.size()+"Sno thread is "+this.sno_in_graph);
//			  		for(int i = 0; i < neighbours.size(); i++)
//			  		{
//			  			neighbour_id = neighbours.get(i);
//			  			System.out.println(this.sno_in_graph +" sno "+"neighbour id is "+neighbour_id+" parent is "+this.parent);
//			  			System.out.println();
//			  			if(neighbour_id != this.parent)
//			  			{
//			  				System.out.println("Sending explore message to neighbor "+neighbour_id+" from "+this.sno_in_graph);
//			  				Message temp_msg= new Message(this.sno_in_graph,this.round+1,this.max_uid,"Explore"); 
//			  				temp_obj = new Messages_in_queue(neighbour_id,temp_msg);
//			  				//temp_obj.node_s_no = neighbour_id;
//							//temp_obj.msg_to_be_sent = temp_msg;
//			  				msgs_in_queues.add(temp_obj);
//			  				//msg_to_be_sent.append(neighbour_id+":"+temp_msg+";");
//			  				
//			  			}
//			  		 }
//			  		}
//			  	//else divide message from the string into arraylist and send msgs to corresponding nodes
//			  	else 
//			  	{
//			  		System.out.println("New Info not true for :::: "+this.sno_in_graph);
//			  		//for node leaf
//			  		if((NACK_Count == neighbours.size() - 1)&&(this.parent != this.sno_in_graph))
//			  		{
//			  			System.out.println("Sending ack message from  "+this.sno_in_graph+" to "+this.parent);
//			  			  Message temp_msg= new Message(this.sno_in_graph,this.round+1,this.max_uid,"ACK"); 
//			  			  temp_obj = new Messages_in_queue(this.parent,temp_msg);
////						  temp_obj.node_s_no = this.parent;
////						  temp_obj.msg_to_be_sent = temp_msg;
//						  msgs_in_queues.add(temp_obj);
//			  		}
//			     	// for leader
//			  		else if(ACK_Count == neighbours.size() )
//			  		{
//			  			System.out.println("Sending Leader message to master by "+this.sno_in_graph);
//			  			Message temp_msg= new Message(this.sno_in_graph,this.round+1,this.max_uid,"Leader"); 
//			  			temp_obj = new Messages_in_queue(0,temp_msg);
////						  temp_obj.node_s_no = 0;
////						  temp_obj.msg_to_be_sent = temp_msg;
//						  msgs_in_queues.add(temp_obj);
//			  			
//			  		}
//			  		//for internal nodes
//			  		else if((NACK_Count + ACK_Count == neighbours.size() - 1)&&(this.parent != this.sno_in_graph))
//			  		{
//			  			System.out.println("Sending ack message from  "+this.sno_in_graph+" to "+this.parent);
//			  			  Message temp_msg= new Message(this.sno_in_graph,this.round+1,this.max_uid,"ACK"); 
//			  			 temp_obj = new Messages_in_queue(this.parent,temp_msg);
////						  temp_obj.node_s_no = this.parent;
////						  temp_obj.msg_to_be_sent = temp_msg;
//						  msgs_in_queues.add(temp_obj);
//			  		}

/*
 * for (int i = 0 ; i < msgs_in_queues.size(); i++) { temp_msg_pbq =
 * masterNode.Data_Messages.get(msgs_in_queues.get(i).GetId());
 * temp_msg_pbq.add(msgs_in_queues.get(i).GetMsg());
 * masterNode.Data_Messages.put(msgs_in_queues.get(i).GetId(), temp_msg_pbq); }
 */
//			  	} 
//			  	for(int i = 0; i <msgs_in_queues.size(); i++)
//			  	{
//			  		System.out.println(msgs_in_queues.get(i).GetId());
//			  	}
//			  	
// Message to master about Round Completion

// while status is FIND DIAMETER, do the find diameter part

// set status to FIND FLOODMAX
// while status is FIND FLOODMAX, do the find floodmax part

// set status to DONE

// System.err.println("The thread will now die");
// end of terminate

// }

/*
 * public void sendMessage(Message msg) { nextRoundMsg.add(msg); }
 * 
 * public enum RoundDone { YES, NO; }
 * 
 * protected RoundDone roundFinishStatus = RoundDone.YES; protected boolean
 * suspendStatus;
 * 
 * /** Implement find diameter function here. Not finished
 * 
 * public void diameter() { // after edge rounds, done.
 * 
 * // First, broadcast distance to all possible nodes. for (Map.Entry<Integer,
 * SlaveThread> n : neighbors.entrySet()) { if (parent != n.getValue())
 * n.getValue().sendMessage(new Message(id, id, diam, MessageType.DIAMETER)); }
 * // then process incoming messages // All processes // upon receiving d from
 * p: // if d+1 < distance: // distance := d+1 // parent := p // send distance
 * to all neighbors }
 * 
 * /** Create a new Message object for each neighbor and send to all of them.
 * Because the messages can be edited later.
 * 
 * @param m
 * 
 * protected void broadcastToNeighbors(Message m) { for (Map.Entry<Integer,
 * SlaveThread> pair : neighbors.entrySet()) { // broadcast to neighbors. Don't
 * broadcast to parents if (pair.getKey() != parent.getId())
 * pair.getValue().sendMessage(new Message(m.getSenderId(), m.getFrom(),
 * m.getDistanceFromTo(), m.getmType())); } }
 * 
 * /** not implemented
 * 
 * @param message
 * 
 * protected void processDiameterMessage(Message message) { // msg is guanrantee
 * to be MessageType.DIAMETER
 * 
 * // if the distance I get is smaller than my distance to that node // or I've
 * never seen that vertex's id before if (message.getDistanceFromTo() + 1 <
 * distance.get(message.getFrom()) || distance.get(message.getFrom()) == null) {
 * distance.put(message.getFrom(), message.getDistanceFromTo() + 1); parent =
 * neighbors.get(message.getSenderId()); // broadcast distance to all neighbords
 * // reuse the incoming message message.setSenderId(id);
 * message.setDistanceFromTo(message.getDistanceFromTo() + 1);
 * broadcastToNeighbors(message); } // else, don't do anything.
 * 
 * }
 * 
 * /** Process different messages in the queue
 * 
 * protected void processMessage(Message message) { if (message.getmType() ==
 * MessageType.EXPLORE) { if (message.getMessageUid() > leaderId) { leaderId =
 * message.getMessageUid(); newInfo = true; } else newInfo = false; } else if
 * (message.getmType() == MessageType.NACK) { // implement
 * 
 * } else if (message.getmType() == MessageType.REJECT) { // implement } else if
 * (message.getmType() == MessageType.DIAMETER) { // if this message is for
 * finding diameter processDiameterMessage(message); } else
 * System.err.println("This message cannot be processed: " +
 * message.getmType().toString());
 * 
 * }
 * 
 * /** Implement Floodmax here. Not finished
 * 
 * protected void floodMax() { round += 1; for (Message m : thisRoundMsg) {
 * processMessage(m); }
 * 
 * if (round == diam) { if (leaderId == id) { masterNode.setLeaderId(leaderId);
 * status = MessageType.IAMLEADER; System.out.println("Leader: " + id + " " +
 * status.toString()); // send message to master } else status =
 * MessageType.NOTLEADER;
 * 
 * if (round < diam && newInfo == true) {
 * 
 * for (Map.Entry<Integer, SlaveThread> pair : neighbors.entrySet()) {
 * pair.getValue().sendMessage(new Message(id, leaderId, MessageType.EXPLORE,
 * round)); } } } }
 * 
 * @Override
 * 
 * 
 * void suspend() { suspendStatus = true; }
 * 
 * public boolean resume() { if (roundFinishStatus.equals(RoundDone.YES)) {
 * suspendStatus = false; notify(); return true; } else return false; }
 * 
 * public MasterThread getMasterNode() { return masterNode; }
 * 
 * public void setMasterNode(MasterThread masterNode) { this.masterNode =
 * masterNode; }
 * 
 * public SlaveThread getParent() { return parent; }
 * 
 * public void setParent(SlaveThread parent) { this.parent = parent; }
 * 
 * public Map<Integer, SlaveThread> getNeighbors() { return neighbors; }
 * 
 * public void setNeighbors(Map<Integer, SlaveThread> neighbors) {
 * this.neighbors = neighbors; }
 * 
 * public RoundDone getStatus() { return roundFinishStatus; }
 * 
 * public void setStatus(RoundDone status) { this.roundFinishStatus = status; }
 * 
 * public RoundDone isFinished() { return getStatus(); }
 * 
 * public void setFinished(RoundDone finished) { this.roundFinishStatus =
 * finished; }
 * 
 * public int getId() { return id; }
 * 
 * public void setId(int id) { this.id = id; }
 * 
 * public int getLeaderId() { return leaderId; }
 * 
 * public void setLeaderId(int leaderId) { this.leaderId = leaderId; }
 * 
 * public int getRound() { return round; }
 * 
 * public void setRound(int round) { this.round = round; }
 * 
 * public void setDiam(int diam) { this.diam = diam; }
 * 
 * /**
 * 
 */
