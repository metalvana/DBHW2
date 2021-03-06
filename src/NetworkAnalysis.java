import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

public class NetworkAnalysis {
   // JDBC driver name and database URL
   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
   static final String DB_URL = "jdbc:mysql://localhost/test";
   //  Database credentials
   static final String USER = "root";
   static final String PASS = "charles";
   static boolean[] marked;
   static int count;

   public static void main(String[] args) {
	   Connection conn = null;
	   String method=args[0];
	   String parameter=args[1];
	   try{
		      Class.forName("com.mysql.jdbc.Driver");
		      conn = DriverManager.getConnection(DB_URL, USER, PASS);
		      
		      //Construct Hash Table (FromNode -> MappedNode)
		      ArrayList<ArrayList<Integer>> nodeTable;
		      nodeTable = ConstructTable(conn);
		      
		      
		      if (method.equals("NeighbourCount")){
				System.out.println(NeighbourCount(Integer.parseInt(parameter),conn));
			  }else if (method.equals("ReachabilityCount")){
//				System.out.println(ReachabilityCount(Integer.parseInt(parameter),conn));
			  }else if (method.equals("DiscoverCliques")){
				System.out.println(DiscoverCliques(Integer.parseInt(parameter),conn, nodeTable));
			  }else if (method.equals("NetworkDiameter")){
				System.out.println(NetworkDiameter(Integer.parseInt(parameter),conn, nodeTable));
			  }
		      

		   }catch(SQLException se){
		      //Handle errors for JDBC
		      se.printStackTrace();
		   }catch(Exception e){
		      //Handle errors for Class.forName
		      e.printStackTrace();
		   }finally{
		      try{
		         if(conn!=null) conn.close();
		      }catch(SQLException se){
		         se.printStackTrace();
		      }//end finally try
		   }
		

	}
   
   private int[] nextNodes(int id, Connection conn){
	 int[] nextNodes =null;
	 int sql_length=0;
	 
	 try{
	   //Execute a query
	   String sql = "SELECT EndNodeID FROM roadNet_CA WHERE FromNodeID="+ id;
	   ResultSet rs = conn.createStatement().executeQuery(sql);
	   //caculate the number of row
	   rs.last();
	   sql_length=rs.getRow();
	   rs.beforeFirst();
	   nextNodes = new int[sql_length];
	   int i=0;
	   while(rs.next()){
		   nextNodes[i]=rs.getInt("EndNodeID");
		   System.out.println(nextNodes[i]);
		   i++;
	   }   	 
	 }catch(SQLException se){
	   //Handle errors for JDBC
	   se.printStackTrace();
	 }
	return Arrays.copyOf(nextNodes, sql_length);  
   }
   private int nextNodesCount(int node, Connection conn){
	   return nextNodes(node,conn).length;
   }
   
   //Construct Hash Table for Each Node(FromNode -> endNode)
   
   public static ArrayList<ArrayList<Integer>> ConstructTable(Connection conn){
	   //construct tmpResult, get length and construct empty ArrayList
	   ArrayList<ArrayList<Integer>> tmpResult = new ArrayList<ArrayList<Integer>>();
	   
	   try{
		   //Execute a query
		   String fromSqlDis = "SELECT DISTINCT FromNodeId FROM roadNet_CA";
		   String fromSql = "SELECT FromNodeId FROM roadNet_CA";
		   String endSql = "SELECT EndNodeId FROM roadNet_CA";
		   ResultSet fromDisSet = conn.createStatement().executeQuery(fromSqlDis);
		   ResultSet fromSet = conn.createStatement().executeQuery(fromSql);
		   ResultSet endSet = conn.createStatement().executeQuery(endSql);
		   
		   //calculate the number of row; push empty rows to tmpResult
		   fromDisSet.last(); Integer arrSize=fromDisSet.getRow(); fromDisSet.beforeFirst();
		   for(int i = 0; i < arrSize; i++){
			   ArrayList<Integer> tmp = new ArrayList<Integer>();
			   tmpResult.add(tmp);
		   }
		   
		   //travel both fromSet and endSet, create hashTable
		   while(fromSet.next() && endSet.next()){
			   tmpResult.get(fromSet.getInt("FromNodeID")).add(endSet.getInt("EndNodeID"));
		   }
		   
		   
		 }catch(SQLException se){
		   //Handle errors for JDBC
		   se.printStackTrace();
		 }
	      
	   return tmpResult;
   }
   
   public static int NeighbourCount(int id, Connection conn){
	 int count=0;
	 try{   
	   //Execute a query
	   String sql = "SELECT EndNodeID FROM roadNet_CA WHERE FromNodeID="+ id;
	   String sql2 = "SELECT FromNodeID FROM roadNet_CA WHERE EndNodeID="+ id;
	   ResultSet rs = conn.createStatement().executeQuery(sql);
	   ResultSet rs2 = conn.createStatement().executeQuery(sql2);
	   //caculate the number of row
	   rs.last();
	   rs2.last();
	   int sql_length=rs.getRow();
	   int sql2_length=rs2.getRow();
	   rs.beforeFirst();
	   rs2.beforeFirst();
//	   System.out.println(sql_length);
//	   System.out.println(sql2_length);   	
	   int [] EndNodes = new int[sql_length];
	   int [] FromNodes = new int[sql2_length];
	   
	   int EndNodesCount=0,FromNodesCount=0;
	   while(rs.next()){
		   EndNodes[EndNodesCount]=rs.getInt("EndNodeID");
//		   System.out.println(EndNodes[EndNodesCount]);
		   EndNodesCount++;
	   }
	   while(rs2.next()){
		   FromNodes[FromNodesCount]=rs2.getInt("FromNodeID");
//		   System.out.println(FromNodes[FromNodesCount]);
		   FromNodesCount++;
	   }
	   
	   int isDiffCount=0;
	   for(int i=0; i<EndNodesCount; i++){
		   for (int j=0; j<FromNodesCount;j++){
			   if (EndNodes[i]==FromNodes[j]){
//				   System.out.println(EndNodes[j]+" ; "+FromNodes[i]);
//				   System.out.println(isDiffCount);
				   isDiffCount++;
			   }
		   }
	   }
	   count=EndNodesCount+FromNodesCount-isDiffCount;
	 }catch(SQLException se){
       //Handle errors for JDBC
       se.printStackTrace();
	 }
	   return count;
   }

   private static void (ArrayList<ArrayList<Integer>> nodeTable, int rootNode) {
	   
	   ArrayList <Integer> neighbor = nodeTable.get(rootNode);

	   if (neighbor.size()>0) {
	       visited [rootNode] = true;
	       
	       for (int w : neighbor){
	           if (!visited[w]) {
	        	   count++;
	               dfs(nodeTable, w);	     
	           }
	       }
	   }  
       return;
   }

   public static int ReachabilityCount(int id, Connection conn){
	   ArrayList<ArrayList<Integer>> nodeTable;
	   nodeTable = constructTable(conn);
	   count=0;
	   dfs(nodeTable,id);
	   System.out.print(count); 
	   
	   return count;
   }
  
/*   public static int ReachabilityCount(int id, Connection conn){
//	   int[] nodes=nextNodes(id,conn);
//	   ArrayList reachableNodes=new ArrayList();
	   ArrayList<>
	   //Execute a query
	   String sql = "SELECT EndNodeID FROM roadNet_CA";
	   String sql2 = "SELECT FromNodeID FROM roadNet_CA";
	   ResultSet rs = conn.createStatement().executeQuery(sql);
	   ResultSet rs2 = conn.createStatement().executeQuery(sql2);
	   //caculate the number of row
	   rs.last();
	   rs2.last();
	   int sql_length=rs.getRow();
	   int sql2_length=rs2.getRow();
	   rs.beforeFirst();
	   rs2.beforeFirst();
	   
	   int [] EndNodes = new int[sql_length];
	   int [] FromNodes = new int[sql2_length];
	   
	   System.out.print(nodes.length); 
	   
	   return 0;
   }*/
   
   
//********************************Combination Helper Functions for Discover Cliques******************************
   public static void combineFunc_helper(ArrayList<ArrayList<Integer>> result, ArrayList<Integer> fromList, int start, ArrayList<Integer> tmpResult, int target){
	   if(tmp.size() == k) result.add(tmp);
	   else{
		   for(int i = start; i < n-target+1; i++){
			   ArrayList<Integer> tmp = (ArrayList<Integer>) tmpResult.clone();
			   tmp.add(fromList.get(i));
			   combineFunc_helper(result, fromList, start, tmp, target-1);
		   }
	   }
   }
   
   public static ArrayList<ArrayList<Integer>> combineFunc(ArrayList<Integer> fromList, int k){
	   ArrayList<ArrayList<Integer>> result = new ArrayList<ArrayList<Integer>>();
	   combineFunc_helper(result, fromList, 0, new ArrayList<Integer>(), k);
	   return result;
   }
   
<<<<<<< Updated upstream
  //tets 
=======
   //testSiyi
>>>>>>> Stashed changes
   public static ArrayList<ArrayList<Integer>> DiscoverCliques(int k, Connection conn){
	   //construct 2d ArrayList, store all combinations
	   ArrayList<ArrayList<Integer>> combResult = new ArrayList<ArrayList<Integer>>();
	   //construct the linked_table at first
	   ArrayList<ArrayList<Integer>> linkTable = ConstructTable(conn);
	   //construct the valid subgraphs(Cliques)
	   ArrayList<ArrayList<Integer>> cliqueResult = new ArrayList<ArrayList<Integer>>();
	   
	   try{
		   //Execute a query
		   String fromSqlDis = "SELECT DISTINCT FromNodeId FROM roadNet_CA";
		   ResultSet fromDisSet = conn.createStatement().executeQuery(fromSqlDis);
		   
		   //travel from set, get ArrayList of FromNode
		   ArrayList<Integer> fromNodeList = new ArrayList<Integer>();
		   while(fromDistSet.next()){
			   fromNodeList.add(fromDisSet.getInt("FromNodeID"));
		   }
		   
		   //find k-combination
		   combResult = combineFunc(fromNodeList, k);
		   
		   //after getting combResult, go over the combResult and insert the valid combination
		   for(int i = 0; i < combResult.size(); i++){
			   boolean validComb = true;
			   ArrayList<Integer> thisRow = combResult.get(i);
			   for(int j = 0; j < thisRow.size(); j++){
				   //Undirected!!!!!!!!!
				   int rowVal = thisRow.get(i);
				   for(int m = 0; m < thisRow.size() && m != j; m++){
					   int tarVal = thisRow.get(m);
					   if(linkTable[rowVal].indexOf(tarVal) == -1) validCom = false;
				   }
			   }
			   if(validComb) cliqueTable.add(thisRow));
		   }
		   
		 }catch(SQLException se){
		   //Handle errors for JDBC
		   se.printStackTrace();
		 }
	      
	   return cliqueTable;
	   
	   //verify the valid combination, delete if not valid
//	   return result;
   }
   
//*******************************************Discover Cliques End********************************************

   

//*******************************************NetworkDiameter Start*******************************************
   
   //build BFS, construct 2D Array to store distance between each node
   public static ArrayList<ArrayList<Integer>> findDistanceTable(ArrayList<ArrayList<Integer>> linkTable){
	   ArrayList<ArrayList<Integer>> result = new ArrayList<ArrayList<Integerr>>();
	   int size = linkTable.size();
	   
	   //2d array full of -1
	   for(int i = 0; i < size; i++)
	   {
		   //construct new tmpRow;
		   ArrayList<Integer> tmpRow = new ArrayList<Integer>();
		   for(int j = 0; j < size; j++){
			   tmpRow.add(-1);
		   }
		   result.add(tmpRow);
	   }
	   
	   //visit tracking array
	   int visitBFS[size] = {0};
	   
	   //doing BFS
	   for(int i = 0; i < size; i)
	   Queue<Integer> nodeQ;
	   
	   
   }
   
   public static int NetworkDiameter(int id, Connection conn){
	   int resultDia = 0;
	   //construct the linked_table at first
	   ArrayList<ArrayList<Integer>> linkTable = ConstructTable(conn);
	   
	   try{
		   //Execute a query
		   String fromSqlDis = "SELECT DISTINCT FromNodeId FROM roadNet_CA";
		   ResultSet fromDisSet = conn.createStatement().executeQuery(fromSqlDis);
		   
		   //calculate the number of row; push empty rows to tmpResult
		   fromDisSet.last(); Integer arrSize=fromDisSet.getRow(); fromDisSet.beforeFirst();
		   for(int i = 0; i < arrSize; i++){
			   ArrayList<Integer> tmp = new ArrayList<Integer>();
			   tmpResult.add(tmp);
		   }
		   
		   //travel both fromSet and endSet, create hashTable
		   while(fromSet.next() && endSet.next()){
			   tmpResult.get(fromSet.getInt("FromNodeID")).add(endSet.getInt("EndNodeID"));
		   }
		   
		   
		 }catch(SQLException se){
		   //Handle errors for JDBC
		   se.printStackTrace();
		 }
   }
   
//*******************************************NetwordDiameter End*********************************************

}
	
