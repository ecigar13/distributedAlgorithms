
## Floodmax implementation (with built-in negative acknowledgement)

**Members**:
 - Gunjan Munjal - gxm171430
 - Sumit Patel - sxp171330
 - Keith Nguyen - kxn161730
The working branches are "master" and "gunjan". keith and keith-globalQueue are experimental. 

**Assumptions:**

 - Unweighted bidrectional graph.
 - Each vertex has a unique integer ID.

**Input:**
 - The input graph is stored in graph.txt.
 - Input format:
	 - First line is the number of vertices. (n)
	 - Second line contains an array of size n with ID of each vertex. 
	 - n +1 lines containing the adjacency matrix. The first row and first column represents the master vertex/node. This node is connected to all other nodes. All the edges on the first column and row have to be 1.

**To run this project, follow these steps:**

 - If you run with eclipse, just clone the two branches above.
 - If you have the tar file, do the steps below:
 - Open Eclipse -> File -> Import -> Projects from Folder or Archive -> Archive option -> navigate to the tar.gz file
 - Pick the second option (submission.tar_expended/floodMax) -> Finish
 - Go to Main.java and run it. If the program throws FileNotFoundException, copy graph.txt into bin file. (this sometimes happens when eclipse doesn't look in the right folder for input).
 - It will take a while to run. Once finished, this will output:
	 - leader
	 - the tree
	 - "Stopping execution" message.
 - The threads will shutdown once the program finishes. If they are still running, it means the program is not finished.

