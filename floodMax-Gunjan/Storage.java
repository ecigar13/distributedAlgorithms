import java.io.*;
import java.util.*;
import java.lang.*;
import java.util.HashMap;
import java.util.Queue;

public class Storage
{


    //HashMap for all the nodes in the system holding queue for each node                                                                                                                                   
    public static Map map = Collections.synchronizedMap(new HashMap<Integer, data> );

    class data
    {
	//int node;
	Queue<> queue = new Queue<message>();

	data(message)
	    {
		//this.node = node;
		queue.push(message);
	    }
    }
}
    