import java.util.*;
import java.io.IOException;
import java.lang.Exception;
import java.util.ArrayList;

public class FloodMax
{
    public start void main(String args[])
    {
	static int round = 0;
	int number_of_nodes = 4;
	Storage str = new Storage();
	int[][] adj_matrix = new int[5][5];
	adj_matrix={ {1,1,1,1,1},{1,0,1,1,0},{1,1,0,0,1},{1,1,0,0,1},{1,0,1,1,0} };
	int Master_UID = 0;
        new Master.start(Master_UID, str, round,number_of_nodes);
	

	for(int UID = 1; UID <= 4; i++)
	    {
		new nodes.start(UID,str, adj_matrix, number_of_nodes);
	    }

    class message
    {
	int UID;
	int max_UID;
	int round;
	String type;
	//ArrayList<> neighbour_nodes  = new ArrayList<Integer>();

	message(int uid, int max_uid, int round , String type)
	{
	    this.UID = uid;
	    this.max_UID = max_uid;
	    this.round = round;
	    this.type = type;
	}

	message(int neighbour)
	{
	    neighbour_nodes.push(neighbour);
	}
	void type()
	{
	    return this.type;
	}
	void round()
	{
	    return this.round;
	}
	void uid()
	{
	    return this.UID;
	}
	void max_UID()
	{
	    return this.max_UID;
	}

    };

   
}
