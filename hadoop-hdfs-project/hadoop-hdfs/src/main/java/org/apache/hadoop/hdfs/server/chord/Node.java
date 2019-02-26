package org.apache.hadoop.hdfs.server.chord;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportReplica;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeInfoProto.AdminState;
import org.apache.hadoop.hdfs.server.balancer.Balancer;
import org.apache.hadoop.hdfs.server.datanode.BPOfferService;

import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNode.DataTransfer;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.util.Daemon;
import org.mortbay.log.Log;









/**
 * Node class that implements the core data structure 
 * and functionalities of a chord node
 * @author Chuan Xia
 *
 */

public class Node {
	
	InetSocketAddress contact_node;
	
	HashSet<Long> msg_id = new HashSet<Long>();
	public boolean changePre = false;
	int wait_time = 0;
	
	public Boolean stopheartbeat = false;
	public boolean shed = false;
	public boolean waitshed = true;
	public String[] shed_blkid = null;
	public String[] trans_blkid = null;
	public Integer trans_transferring_blk_num = 0;
	public Integer shed_transferring_blk_num = 0;
	
	HashMap<String,Long> id_modified_server = new HashMap<String,Long>();
	HashMap<String,Long> id_server = new HashMap<String,Long>();
	public int message_num = 0;
	public long movement_cost = 0L;
	public int balancing_round = 0;
	public int num_rejoin = 0;
	
	public DataNode dn;
	boolean trans_terminate = false;
	boolean transfer = false;
	public long localId;
	boolean id_modified = false;
	public boolean initializing = true;
	
	public boolean shed_transferring = false;
	public boolean shed_receiving = false;
	public boolean trans_transferring = false;
	public boolean trans_receiving = false;
	
	public InetSocketAddress localAddress;
	public InetSocketAddress predecessor;
	public InetSocketAddress new_pred;
	public InetSocketAddress trans_new_succ;
	public InetSocketAddress shed_old_pre;
	public InetSocketAddress shed_old_succ;
	
	public HashMap<Integer, InetSocketAddress> finger;
	public HashMap<InetSocketAddress,Long> request_nodes;
	public int MAX_REQUEST = 3;
	
	public boolean waitforrequest = false;
	public boolean getResponse = false;//response from heavy node for share load request 
	
	public boolean balancing = false;// already in balancing status or not begin balancing
	
	public boolean agreed = false;// be chosen from heavy node
	

	public Listener listener;//ip:port
	public Stabilize stabilize;
	public FixFingers fix_fingers;
	public AskPredecessor ask_predecessor;
	
	public class Vector{
		public int version;
		
		/*
		 *   public String ipAddr;     // IP address
  		 *	 public String hostName;   // hostname claimed by datanode
  		 *	 public String peerHostName; // hostname from the actual connection
  		 *	 public int xferPort;      // data streaming port
  		 *	 public int infoPort;      // info server port
  		 *	 public int infoSecurePort; // info server port
  		 *	 public int ipcPort;       // IPC server port
  		 *	 public String xferAddr;
		 *	 public final String datanodeUuid;
		 */
		
		/*
		 * 1 item of table consists of 4 components
		 * ID ---> node info(Cap, Used, ip:port)
		 * [ID, Capacity DFSUsed ip:port]
		 */
		public Map<String, String> table;
		public final int MAX_NUM = 10;
		
		public void add(String id, String nodeinfo) {
			table.put(id, nodeinfo);
		}
		
		Vector(){
			version = 0;
			table = new HashMap<String, String>();
		}
		
		
		Vector(int v, Map<String, String> t){
			version = v;
			table = t;
		}
		
		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 * { version@[id cap used ip:port],[id cap used ip:port] ... }
		 */
		public String toString() {
			String result = version + "@";
			int i=1;
			for(Entry<String,String> entry: table.entrySet()) {
				String id = entry.getKey().toString();
				String nodeinfo = entry.getValue();
				if(i==1)result = result + id + " " + nodeinfo;
				else result = result + "," + id + " " + nodeinfo;
				i++;
				
			}
			return result;
			
		}

		
	}
	
	public Vector vector = new Vector();
	/**
	 * Constructor
	 * @param address: this node's local address
	 * @throws InterruptedException 
	 */
//-------------------------------------------------------------------------------------------------------------------------------

	
	public boolean shedLoadToSucc(InetSocketAddress new_pre) throws InterruptedException {

		System.out.println("waiting for successor updating");
		while(finger.get(1) == null) {
			Thread.sleep(100);
		}
		System.out.println("update succesor done");
		InetSocketAddress shed_receiver = finger.get(1);
		shed_old_succ = finger.get(1);
		System.out.println("shed load to succ begin");
		long size = 0L;
		String suc_id = null;

		suc_id = Helper.sendRequest(shed_receiver, "YOURUUID");
		Log.info("Now the successor is "+shed_receiver.getHostName());
		Log.info("The Uuid is "+suc_id);

		message_num++;
		List<BPOfferService> Bpos_list = dn.getAllBpOs();
		for(BPOfferService bpos: Bpos_list) {
			Map<DatanodeStorage, BlockListAsLongs> perVolumeBlockLists = 
			dn.getFSDataset().getBlockReports(bpos.getBlockPoolId());
			String poolId = bpos.getBlockPoolId();
			for(Map.Entry<DatanodeStorage, BlockListAsLongs> entry: perVolumeBlockLists.entrySet()) {
				
				BlockListAsLongs bl = entry.getValue();
				Block[] blocks = new Block[bl.getNumberOfBlocks()];
				int i = 0;
				for(BlockReportReplica b : bl) {
					blocks[i] = new Block(b);
					i++;
					size += b.getNumBytes();
				}
				
				String addr = null;
				try {
					addr = InetAddress.getLocalHost().getHostAddress()+":"+localAddress.getPort();
				} catch (UnknownHostException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				System.out.println("there are "+i+" blocks to be transferred");
				String shedinfo = "READYTOSHED_"+i+"_"+dn.id.datanodeUuid+"_"+addr;
				shed_blkid = new String[blocks.length];
				for(int i1=0;i1<blocks.length;i1++) {
					shed_blkid[i1] = String.valueOf(blocks[i1].blockId);
					shedinfo += "_"+blocks[i1].blockId;
					System.out.println("transfer block:"+blocks[i1].blockId);
				}
				shed_transferring_blk_num = blocks.length;

				String resp = null;

				
				System.out.println("send shed request to successor");
				resp = Helper.sendRequest(shed_receiver, shedinfo);
					
				System.out.println("response is "+resp);
				
				message_num++;
				
				long start = System.currentTimeMillis();
				
				while(resp.equals("INITIALIZING")) {
					System.out.println("waiting for successor response");
					Thread.sleep(500);
					System.out.println("successor updating");
					
					System.out.println("update successor done");
					resp = Helper.sendRequest(shed_receiver, shedinfo);
					System.out.println("response is "+resp+"\n");
					message_num++;
					long end = System.currentTimeMillis();
					long time = (end - start) / 1000;
					System.out.println("current waiting time is "+time+"s");
					if(time > 30) {
						System.out.println("waiting for READYTOSHED response timeout, exit");
						System.out.println("send \"STOPWAITING\" message to heavy node");
						Helper.sendRequest(new_pre, "STOPWAITING");
						//this.trans_transferring = false;
						this.trans_receiving = false;
						this.shed_transferring = false;
						//this.shed_receiving = false;
						this.shed = false;
						dn.receiving = false;
						
						return false;
					}
				}
				System.out.println("successor is ready to shed receiving");
				
				DatanodeInfo[] Targets = new DatanodeInfo[1];
				Vector other = null;
				if(!vector.table.containsKey(suc_id)) {
					Log.info("the vector does not contain suc_id");
					String ret;
					boolean f = true;
					System.out.println("waiting for receiver's information");
					do {
						
							ret = Helper.sendRequest(shed_receiver, "YOURVECTOR shed_trans");
						
						message_num++;
						if(ret != null)
							if(!ret.startsWith("INITIALIZING"))
							f = false;
						
						Thread.sleep(100);
					}while(f);
					System.out.println("get receiver's information");
					other = build(ret);
					Targets[0] = build_NodeInfo(suc_id,other);
				}
				
				else {					
					//other = build(vector.table.get(suc_id));
					Log.info("the vector contains suc_id");
					Targets[0] = build_NodeInfo(suc_id,other);					
				}
				StorageType[] TargetStorageTypes = new StorageType[1];
				TargetStorageTypes[0] = StorageType.DISK;
				movement_cost += size;
				System.out.println("I will shed shoad to my succ:"+shed_receiver.getHostName()
						+", this time movement cost = "+size+", in total the movenment size = "
						+movement_cost/(1024*1024)+"M");
				System.out.println("\n\n");
				//}
				try {
					for(int i1=0;i1<blocks.length;i1++) {
						System.out.println("this time transfer block:"+blocks[i1].getBlockId()+", in block pool:"+poolId);
						Log.info("target node is:"+Targets[0].xferAddr);
						dn.transferBlock( new ExtendedBlock(poolId, blocks[i1]), Targets, TargetStorageTypes);
						//dn.metrics.incrBlocksRemoved(toDelete.length);
						
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			//ExtendedBlock Eblock = new ExtendedBlock(poolId, blocks[i])
			//void transferBlock(ExtendedBlock Eblock, DatanodeInfo[] xferTargets,
		    //StorageType[] xferTargetStorageTypes)
			//new Daemon(new DataTransfer(xferTargets, xferTargetStorageTypes, block,
	        //BlockConstructionStage.PIPELINE_SETUP_CREATE, "")).start();
		}
		return true;
	}
	
	public String requestLoadFromPre(long size, InetSocketAddress new_pre) throws UnknownHostException{
		String addr = InetAddress.getLocalHost().getHostAddress()+":"+localAddress.getPort();
		message_num++;
		return Helper.sendRequest(new_pre, "SHARELOAD,"+size+","+localId+","+addr);	
	}
	public void transferData(String id, long size, InetSocketAddress ip) {//finger contain or vector contain
				
		//List<BPOfferService> Bpos_list = dn.getAllBpOs();
		long schedule = size;
		boolean flag = false;
		for(Entry<String, BPOfferService> e: dn.blockPoolManager.bpByBlockPoolId.entrySet()) {
			if(flag) break;
			BPOfferService bpos = e.getValue();
			Map<DatanodeStorage, BlockListAsLongs> perVolumeBlockLists = 
			dn.getFSDataset().getBlockReports(bpos.getBlockPoolId());
			String poolId = e.getKey();
			for(Map.Entry<DatanodeStorage, BlockListAsLongs> entry: perVolumeBlockLists.entrySet()) {
				
				BlockListAsLongs bl = entry.getValue();
				List<Block> block_list = new ArrayList<Block>();
				
				
				for(Block b : bl) {
					System.out.println("in the second out loop, block id = "+b.blockId);
					boolean contained = false;
					long bl_size = b.getNumBytes();
					System.out.println("schedule size = "+schedule);
					System.out.println("block id = "+b.getBlockId()+", block size = "+b.getNumBytes());
					for(Block blk: block_list) {
						//System.out.println("block list contains block: "+blk.getBlockId());
						if(blk.getBlockId() == b.getBlockId()) {
							//System.out.println("block as longs contains block id = "+b.getBlockId()+" has already contained in block_list");
							contained = true;
						}
					}
					
					
					if(schedule > bl_size && !contained) {
						Block clone_block = new Block(b);
						//System.out.println("clone block id = "+clone_block.blockId);
						//System.out.println("original block id = "+b.blockId);
						block_list.add(clone_block);
						schedule -= bl_size;
					}
					else if(schedule < bl_size){
						flag = true;
						break;
					}
					
				}
				System.out.println("there are "+block_list.size()+" blocks to be transferred");
				if(block_list.size()==0) {
					System.out.println("no blocks transfer");
					this.trans_transferring = false;
				}
				Block[] blocks = new Block[block_list.size()];
				String trans_info = "TRANSFERBLOCK";
				trans_blkid = new String[blocks.length];
				for(int i=0;i<blocks.length;i++) { 
					blocks[i] = block_list.get(i);
					trans_blkid[i] = String.valueOf(blocks[i].blockId);
					trans_info += "_"+blocks[i].blockId;
				}
				trans_transferring_blk_num = blocks.length;
				Helper.sendRequest(ip, trans_info);
				message_num++;
				
				DatanodeInfo[] Targets = new DatanodeInfo[1];
				Vector other = null;
				if(!vector.table.containsKey(id)) {
					boolean f =true;
					String ret;
					System.out.println("waiting for trans receiver's information");
					do {
						ret = Helper.sendRequest(ip, "YOURVECTOR trans_trans");
						message_num++;
						
						if(ret != null)
							if(!ret.startsWith("INITIALIZING"))
								f =false;
							try {
								Thread.sleep(300);
							} catch (InterruptedException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
					}while(f);
					System.out.println("get the receiver's information");
					
					other = build(ret);
					Targets[0] = build_NodeInfo(id,other);
				}
				
				else {
					//other = vector.table.get(id);
					Targets[0] = build_NodeInfo(id,other);
				}
				
				StorageType[] TargetStorageTypes = new StorageType[1];
				TargetStorageTypes[0] = StorageType.DISK;
				
				for(int i=0;i<blocks.length;i++) {
					try {
						System.out.println("this time transfer block:"+blocks[i].getBlockId()+", in block pool:"+poolId);
						dn.transferBlock(new ExtendedBlock(poolId, blocks[i]), Targets, TargetStorageTypes);
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						System.out.println("exception in transferring block: "+blocks[i].getBlockId()
											+ "block pool id: "+poolId+" target: "+Targets.toString());
						e1.printStackTrace();
					}
					/*
					try {
				        // using global fsdataset
						Block[] toDelete = {blocks[i]};
				        dn.getFSDataset().invalidate(poolId, toDelete);
				      } catch(IOException e1) {
				        // Exceptions caught here are not expected to be disk-related.
				        e1.printStackTrace();
				      }
					//dn.metrics.incrBlocksRemoved(toDelete.length);
					 */
					 
				}
				
				
				//transferBlock(new ExtendedBlock(poolId, blocks[i]), xferTargets[i],
			    //        xferTargetStorageTypes[i]);
				
				
				
				
				
			}
			
		}
	}
	
	
	
	 class DN{
		public String id;
		public long used;
		DN(String id, long used){
			this.id = id;
			this.used = used; 
		}
	}
	class cmp implements Comparator<DN>{
		public int compare(DN a, DN b) {
			if(a.used<b.used) return -1;
			else if(a.used==b.used) return 0;
			else return 1;
		}
	}
	public DN[] getDNs() {
		DN[] dn_list = new DN[vector.table.size()];
		int n=0;
		for(Entry<String, String> e: vector.table.entrySet()) {
			String id = e.getKey();
			String nodeinfo = e.getValue();
			String[] partition = nodeinfo.split(" ");
			long used = Long.parseLong(partition[1]);
			dn_list[n] = new DN(id,used);
			n++;
 		}
		Arrays.sort(dn_list, new cmp());
		return dn_list;
	}
	public int find_pos(String id, DN[] dn_list) {
		for(int i=0;i<dn_list.length;i++)
			if(id.equals(dn_list[i].id))
				return i+1;
		return -1;
	}
	
	public void initialize_all_parameters() {
		
		trans_transferring_blk_num = 0;
		shed_transferring_blk_num = 0;
		trans_terminate = false;
		shed_transferring = false;
		shed_receiving = false;
		trans_transferring = false;
		trans_receiving = false;
		
		shed_blkid = null;
		trans_blkid = null;
		
		id_modified_server = new HashMap<String,Long>();
		id_server = new HashMap<String,Long>();
		
		transfer = false;
		id_modified = false;

		trans_new_succ = null;
		new_pred = null;
		shed_old_pre = null;
		shed_old_succ = null;
		
		request_nodes = new HashMap<InetSocketAddress,Long>();
		
		waitforrequest = false;
		getResponse = false;//response from heavy node for share load request 
		
		balancing = true;// already in balancing status or not begin balancing
		
		agreed = false;// be chosen from heavy node
		
		dn.shed_ready_to_receive = false;
		dn.ready_to_receive = false;
		dn.ready_to_transfer = false;
		  
		dn.shed_receiving_blocks = null;
		dn.shed_receiving = false;
		  
		dn.receiving_blocks = null;
		dn.receiving = false;
	}
public void initialize_parameters() {
		trans_terminate = false;
		trans_transferring_blk_num = 0;
		shed_transferring_blk_num = 0;
		shed_transferring = false;
		//shed_receiving = false;
		trans_transferring = false;
		//trans_receiving = false;
		
		shed_blkid = null;
		trans_blkid = null;
		
		id_modified_server = new HashMap<String,Long>();
		id_server = new HashMap<String,Long>();
		
		transfer = false;
		//id_modified = false;

		//trans_new_succ = null;
		new_pred = null;
		//shed_old_pre = predecessor;
		//shed_old_succ = finger.get(1);
		
		request_nodes = new HashMap<InetSocketAddress,Long>();
		
		waitforrequest = false;
		getResponse = false;//response from heavy node for share load request 
		
		balancing = true;// already in balancing status or not begin balancing
		
		agreed = false;// be chosen from heavy node
		
		dn.shed_ready_to_receive = false;
		dn.ready_to_receive = false;
		dn.ready_to_transfer = false;
		  
		//dn.shed_receiving_blocks = null;
		//dn.shed_receiving = false;
		  
		dn.receiving_blocks = null;
		dn.receiving = false;
	}
	
	public void loadbalance() throws InterruptedException, IOException {
		
		InetSocketAddress NameNode = Helper.createSocketAddress(dn.NNip+":22222");
		Log.info("dn.id.datanodeUuid is "+dn.id.datanodeUuid);
		Log.info("dn.storage.datanodeUuid is "+dn.storage.getDatanodeUuid());
		System.out.println("\n\n\n\n\n\n\n\n");
		System.out.println("began balancing");
		long start = System.currentTimeMillis();
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
		
		int blk_num = (int) (this.getDFSUsed()/(1024*1024*16));
		
		String init_message = dn.id.datanodeUuid + ","
				 			  +blk_num + ","
				 			  +movement_cost + ","
				 			  +message_num + ","
				 			  +"0,"
				 			  +localAddress.getPort()+","
				 			  +"0";
		Helper.sendRequest(NameNode, "LBMESSAGE,"+init_message);
		
		initialize_all_parameters();
		
		Thread.sleep(1000*10);
		do {
			shed = false;
			System.out.println("datanode "+this.localAddress.getHostName()
					+" begin balancing\nbalancing round = "+balancing_round);
		balancing_round++;
		cal = Calendar.getInstance();
		System.out.println("load balance start at "+sdf.format(cal.getTime()));
		System.out.println("check shed_receiving");
		
		
		while(shed_receiving) {
			Thread.sleep(150);
		}
		System.out.println("shed done");
		initialize_parameters();
		
		
		while(finger.get(1)==null||predecessor==null) {
			Thread.sleep(100);
		}
		synchronized(finger.get(1)) {
			synchronized(predecessor) {
				System.out.println("\n\n\n");
				System.out.println("this node's sucessor is "+finger.get(1).getHostName());
				System.out.println("predecessor is "+predecessor.getHostName());
				System.out.println("\n\n\n");
			}
		}
		
		initializeVector();
		System.out.println("initialize vector done");
		System.out.println("There are "+vector.table.size()+" samples");
		initializing = false;
		randomlyselect();
		System.out.println("random select done");
		System.out.println("There are "+vector.table.size()+" samples");
		boolean success = gossipProtocal();
		System.out.println("gossip protocal done");
		System.out.println("There are "+vector.table.size()+" samples");
		
		boolean consistency = true;
		System.out.println("consistentcy check");
		HashSet<String> dirty_keys = new HashSet<String>();
		for(Entry<String, String> e: vector.table.entrySet()) {
			String nodeinfo = e.getValue();
			
			String tar_Uuid = nodeinfo.split(" ")[10];
			String my_Uuid = dn.getDatanodeUuid();
			if(my_Uuid.equals(tar_Uuid)) continue;
			
			String ip = nodeinfo.split(" ")[2];
			String port = nodeinfo.split(" ")[15];

			Long usage = Long.valueOf(nodeinfo.split(" ")[1]);
			InetSocketAddress target =  Helper.createSocketAddress(ip+":"+port);
			String cur_usage_s = Helper.sendRequest(target, "YOURDFSUSED");
			long cur_usage = Long.parseLong(cur_usage_s);
			if(usage != cur_usage) {
				//consistency = false;
				System.out.println("sample of "+ip+":"+port+" is inconsistent, drop item");
				System.out.println("sampling is "+(usage/(1024*1024*16))+" blocks, actual number is "+(cur_usage/(1024*1024*16))+" blocks");
				//vector.table.remove(e.getKey());
				dirty_keys.add(e.getKey());
			}
		}
		for(String key : dirty_keys) {
			vector.table.remove(key);
		}
		
		long average = getAverage();
		System.out.println("the average is "+average);
		long percent = getDFSUsed();
		System.out.println("dfs used = "+percent);
		DN[] dn_list = getDNs();
		for(int i=0;i<dn_list.length;i++) {
			String nodeinfo = vector.table.get(dn_list[i].id);
			String port = nodeinfo.split(" ")[15];
			String ip = nodeinfo.split(" ")[2];
			System.out.println((i+1)+" node is " +ip+":"+port+"\tusage is "+dn_list[i].used);
			
		}
		
		
		
		int num_light = 0;
		int num_heavy = 0;
		
		
		for(int n=0;n<dn_list.length;n++) {
			if(dn_list[n].used+0.2*average < average) num_light++;
			if((dn_list[n].used-0.2*average)>average) num_heavy++;
		}
		
		DN[] light_nodes = new DN[num_light];
		for(int i=0;i<num_light;i++)
			light_nodes[i] = dn_list[i];
		
		
		DN[] heavy_nodes = new DN[num_heavy];
		for(int i=0;i<num_heavy;++i) {
			int size_dn = dn_list.length;
			heavy_nodes[i] = dn_list[size_dn-1-i];
		}

		for(int i=0;i<light_nodes.length;i++) {
			String nodeinfo = vector.table.get(light_nodes[i].id);
			String port = nodeinfo.split(" ")[15];
			String ip = nodeinfo.split(" ")[2];
			System.out.println((i+1)+" light node is "+ ip+":"+port
					+"\tusage is "+(light_nodes[i].used/(1024*1024*16))+" blocks");
		}
		
		for(int i=0;i<heavy_nodes.length;i++) {
			String nodeinfo = vector.table.get(heavy_nodes[i].id);
			String port = nodeinfo.split(" ")[15];
			String ip = nodeinfo.split(" ")[2];
			System.out.println((i+1)+" heavy node is "+ip+":"+port
					+"\tusage is "+(heavy_nodes[i].used/(1024*1024*16))+" blocks");
		}
		
		boolean light = (percent + 0.2 * average) < average;
		boolean heavy = (percent - 0.2 * average) > average;
		
		

		boolean non_match = ((heavy_nodes.length == 0) && (light_nodes.length > 0)) ||
							((light_nodes.length == 0) && (heavy_nodes.length > 0)) ;
		
		if(!success || non_match) {
			consistency = false;
			if(!success)
				System.out.println("gossip sampling fails");
			if(non_match)
				System.out.println("#heavy_node is 0 or #light_node is 0");
		}
		
		if(!consistency||shed_receiving) {
			Log.info("vector infomation is inconsistent, leaving this round load balancing");
			initializing = true;
			waitforrequest = true;
			Log.info("check if this node has shedding load to receive");
			if(shed_receiving)
				Log.info("this node is in shed_receiving state, waiting for finishing");
			
			
			shed = true;
			
			
			Thread.sleep(3*1000);
			System.out.println("waiting loop");
			System.out.println("shed_trans: "+shed_transferring);
			System.out.println("shed_rec: "+shed_receiving);
			System.out.println("trans_trans: "+trans_transferring);
			System.out.println("trans_rec: "+trans_receiving);
			long s = 0;
			while(	   shed_transferring 
					|| shed_receiving 
					|| trans_receiving 
					|| trans_transferring) {
				
				Thread.sleep(500);
				
				
				if(count(shed_transferring,shed_receiving,trans_receiving,trans_transferring) == 1 && s == 0) {
					s = System.currentTimeMillis();
				}
				long e = System.currentTimeMillis();
				long t = 0;
				if(s!=0)
					t = (e - s) / 1000;
				if(t > 30) {
					System.out.println("exceed time out, break");
					initialize_state();
					break;
					
				}

			}
			
			System.out.println("wait for update successor or predecessor");
			while(finger.get(1)== null|| predecessor==null) {
				Thread.sleep(100);
			}
			System.out.println("update successor or predecessor done");
			synchronized(finger.get(1)) {
				synchronized(predecessor) {
					Log.info("no shed_receiving or shed_receiving is finished");
					System.out.println("this node is "+localAddress.getHostName()
										+" after balancing, this node's sucessor is " + finger.get(1).getHostName()
										+", predecessor is "+predecessor.getHostName());
					System.out.println("\n\n\n");
				}
			}
			
			System.out.println("After this round, movement cost is "+movement_cost/(1024*1024)+"M"+", message number is "+message_num);
			System.out.println("\n\n\n");
			
			long end = System.currentTimeMillis();
			cal = Calendar.getInstance();
			System.out.println("one round load balance done at "+sdf.format(cal.getTime()));
			long et = (end - start)/1000;
			System.out.println("elapsed time is "+et+" seconds");
			
			continue;
		}
		
		
		
		if(light) {
			initializing = true;
			shed = true;
			System.out.println("this node belongs to light node");
			int k1 = find_k1(light_nodes, dn.getDatanodeUuid());
			System.out.println("k1 = "+k1);
			String result = compute_k2( heavy_nodes, average, k1);
			if(result==null)
				continue;
			String[] partition = result.split(":");
			int k2 = Integer.parseInt(partition[0]);
			long move_size = Long.parseLong(partition[1]);
			System.out.println("k2 = "+k2);
			System.out.println("move size = "+move_size);
			String id = heavy_nodes[k2-1].id;
			String[] nodeinfos = vector.table.get(id).split(" ");
			String port = nodeinfos[15];
			String ip = nodeinfos[2];
			System.out.println("chosen heavy node is:"+ip+":"+port);
			InetSocketAddress new_pre = Helper.createSocketAddress(ip+":"+port);
			
			boolean NoShed = comparePre_Suc(new_pre);
			if(NoShed) {
				System.out.println("no shed");
				long cur_usage = getDFSUsed();
				long below_avg = average - cur_usage;
				if(move_size > below_avg) {
					System.out.println("old move size: "+move_size);
					move_size = below_avg;
					System.out.println("new move size: "+move_size);
				}
				else {
					System.out.println("move size unchanged");
				}
			}
			leaveAndrejoin(new_pre, move_size,NoShed);
			System.out.println("\n\n\n");
			System.out.println("leave and rejoin done");
			System.out.println("\n\n\n");
			
		}
		
		
		
		else if(heavy) {
			shed = true;
			initializing = false;
			System.out.println("this node belongs to heavy node");
			transfer = false;
			System.out.println("waiting for share load request");
			
			waitforrequest = true;
			for(int i=0; i<60 && request_nodes.size()<1 && waitforrequest;i++) {
				System.out.println("waiting for request nodes");
				Thread.sleep(100);
			}
			waitforrequest = false;
			initializing = true;
			
			if(!transfer && request_nodes.size()>=1) {
				trans_transferring = true;
				dn.ready_to_receive = false;
				int pick = (int) (Math.random()*request_nodes.size());
				Set<InetSocketAddress> ks = request_nodes.keySet();
				InetSocketAddress[] a = ks.toArray(new InetSocketAddress[0]);
				
				String picked_ip = a[pick].getAddress().toString().split("/")[1];
				String picked_port = String.valueOf(a[pick].getPort());
				
				InetSocketAddress picked_node = Helper.createSocketAddress(picked_ip+":"+picked_port);
				long size = request_nodes.get(picked_node);
				String id = Helper.sendRequest(picked_node, "YOURUUID");
				message_num++;
				transfer = true;
				
				Helper.sendRequest(picked_node, "ACC");
				message_num++;
				request_nodes.remove(picked_node);
				
				if(!request_nodes.isEmpty()) {
					for(InetSocketAddress s:request_nodes.keySet()) {
						Helper.sendRequest(s, "REJ");
						message_num++;
						//request_nodes.remove(s);
					}
					request_nodes.clear();
				}
				if(picked_node != null) {
					System.out.println("\n\nthe picked node is NOT null\n\n");
				}
				else {
					System.out.println("\n\nthe picked node is NULL\n\n");
				}
				
				
				System.out.println("waiting for light node shed");
				trans_new_succ = picked_node;
				while(!dn.ready_to_receive && waitshed) {
					Thread.sleep(500);
				}
				
				if(waitshed) {
					System.out.println("light node shed done");
					transferData(id,size,picked_node);
				}
				else {
					waitshed = true;
				}

				dn.ready_to_receive = false;
			}
			
			else {
				if(transfer)System.out.println("this node has already picked one light node");
				if(request_nodes.size()<1) {
					System.out.println("does not get a light node request");
					trans_transferring = false;
				}
			}
		}
		else {
			shed = true;
			System.out.println("this node belongs to balanced node");
		}
		
		shed = true;
		Thread.sleep(3000);
		initializing = true;
		

		System.out.println("waiting loop");
		System.out.println("shed_trans: "+shed_transferring);
		System.out.println("shed_rec: "+shed_receiving);
		System.out.println("trans_trans: "+trans_transferring);
		System.out.println("trans_rec: "+trans_receiving);
		long s = 0;
		while(	   shed_transferring 
				|| shed_receiving 
				|| trans_receiving 
				|| trans_transferring) {
			
			Thread.sleep(500);
			
			
			if(count(shed_transferring,shed_receiving,trans_receiving,trans_transferring) == 1 && s == 0) {
				s = System.currentTimeMillis();
			}
			long e = System.currentTimeMillis();
			long t = 0;
			if(s!=0)
				t = (e - s) / 1000;
			if(t > 30) {
				System.out.println("exceed time out, break");
				initialize_state();
				break;
				
			}

		}
		

		while(finger.get(1)== null|| predecessor==null) {
			Thread.sleep(100);
		}
		synchronized(finger.get(1)) {
			synchronized(predecessor) {
					System.out.println("this node is"+localAddress.getHostName()
									+" after balancing, this node's sucessor is " + finger.get(1).getHostName()
									+", predecessor is "+predecessor.getHostName());
					System.out.println("\n\n\n");
			}
		}
		System.out.println("After this round, movement cost is "+movement_cost/(1024*1024)+"M"+
							", message number is "+message_num);
		int block_num = (int) (this.getDFSUsed()/(1024*1024*16));
		System.out.println("This node has "+(this.getDFSUsed()/(1024*1024*16))+" blocks");
		System.out.println("\n\n\n");
		
		long end = System.currentTimeMillis();
		cal = Calendar.getInstance();
		System.out.println("one round load balance done at "+sdf.format(cal.getTime()));
		long et = (end - start)/1000;
		System.out.println("elapsed time is "+et+" seconds");

		System.out.println("send load banlance message to namenode");
		String message = dn.id.datanodeUuid + ","
						 +block_num + ","
						 +movement_cost/(1024*1024) + ","
						 +message_num + ","
						 +et+","
						 +localAddress.getPort()+","
						 +num_rejoin;
		Helper.sendRequest(NameNode, "LBMESSAGE,"+message);
		
		}while(balancing);
		
	}
	
	public int count(boolean a, boolean b, boolean c, boolean d) {
		int i=0;
		if(a)i++;
		if(b)i++;
		if(c)i++;
		if(d)i++;
		return i;
	}
	
	public void initialize_state(){
		trans_transferring = false;
		trans_receiving = false;
		shed_receiving = false;
		shed_transferring = false;
	}
	
public boolean comparePre_Suc(InetSocketAddress new_pre) {
		// TODO Auto-generated method stub
	
	System.out.println("successor or predecessor updating");
	while(finger.get(1) == null || predecessor == null) {
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	System.out.println("update successor or predecessor done");
	
		String Pre_Uuid = Helper.sendRequest(predecessor, "YOURUUID");
		String Suc_Uuid = Helper.sendRequest(finger.get(1), "YOURUUID");
		String new_Pre_Uuid = Helper.sendRequest(new_pre, "YOURUUID");
		message_num += 3;
		return new_Pre_Uuid.equals(Pre_Uuid) || new_Pre_Uuid.equals(Suc_Uuid);
	}

public void leaveAndrejoin(InetSocketAddress new_pre, long move_size,boolean NoShed) throws IOException, InterruptedException {
		System.out.println("leave and rejoin begin!");
		  /*
		   * 1. shed all load to current succ
		   * 2. shutdownallthread
		   * 3. rejoin determine new pre and new succ
		   * 4.request load from new pre
		   * 
		   * 
		   * 
		   */
		  InetSocketAddress addr = getAddress();
		  String port = String.valueOf(addr.getPort());
		  String local_ip = null;
		  Node.Vector v = vector;
		try {
			
			local_ip = InetAddress.getLocalHost().getHostAddress();
			
		} catch (UnknownHostException e) {
			
			e.printStackTrace();
		}
		System.out.println("requesting load from pre");
		String resp = requestLoadFromPre(move_size, new_pre);
		System.out.println("result is "+resp);
		  waitforrequest = true;
		  for(int i=0;i<10 && resp.equals("INITIALIZING");i++) {
			try {
				Thread.sleep(600);
				System.out.println("requesting load from pre");
				resp = requestLoadFromPre(move_size, new_pre);
				System.out.println("result is "+resp);
		
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			};
		  }
		  System.out.println("requesting load from pre is done");
		  waitforrequest = false;
		  
		  if(resp.equals("INITIALIZING")) {
			  System.out.println("heavy node is initializing");
			  agreed = false;
			  getResponse = false;
		  }
		  else if(resp.equals("COPYTHAT")) {
			  System.out.println("heavy node get the request, wait for picking");
			  while(!getResponse) {
				  Thread.sleep(1000);
			  }
		  }
		  
		  if(agreed && getResponse) {
			  boolean shed_success = true;
			  System.out.println("get approved by heavy node");
			  System.out.println("successor updating");
			  while(finger.get(1)==null) {
				  Thread.sleep(100);
			  }
			  System.out.println("update successor done");
			  //shed_old_succ = finger.get(1);
			 
			  if(!NoShed) {
				  System.out.println("this light node needs to shed load to its successor");
				  shed_transferring = true;
			  }
			  else {
				  System.out.println("no need to shed load");
				  shed_transferring = false;
			  }
			  trans_receiving = true;
			  movement_cost += move_size;
			  this.new_pred = new_pre;
			  dn.receiving = true;
			  
			  
			  
			  if(NoShed || this.getDFSUsed() < 1024*1024*16) {
				  System.out.println("successor updating");
					while(finger.get(1)==null) {
						Thread.sleep(100);
					}
					System.out.println("update successor done");
				  synchronized(finger.get(1)) {
					  Helper.sendRequest(finger.get(1), "SHEDTRANSFIN");
				  }
				  shed_transferring = false;
			  }
			  
			  else {
				  
				  System.out.println("waiting for shed receiving");
				  while(shed_receiving) {
					  Thread.sleep(300);
				  }
				  
				  System.out.println("shed receiving finish");
				  shed_success = shedLoadToSucc(new_pre);
			  }
			  System.out.println("waiting for shed transferring");
			  long s = System.currentTimeMillis();
			  while(shed_transferring) {//during shed_transfer, light node may incorrectly delete shed blocks
				  						//so set up timeout to exit if that happens
				  Thread.sleep(500);
				  long e = System.currentTimeMillis();
				  long time = (e - s) / 1000;
				  System.out.println("current waiting time is "+time+"s");
				  if(time > 30) {
					  System.out.println("waiting for SHEDTRANSFER timeout, exit");
					  System.out.println("send \"STOPWAITING\" message to heavy node");
					  Helper.sendRequest(new_pre, "STOPWAITING");
					  //this.trans_transferring = false;
					  this.trans_receiving = false;
					  this.shed_transferring = false;
					  //this.shed_receiving = false;
					  this.shed = false;
					  dn.receiving = false;
					  shed_success = false;
					  
					}
			  }
			  System.out.println("shed transferring done");
			
			  if(shed_success) {
				  Helper.sendRequest(new_pre, "READYTORECEIVE_TRANSFER");
				  num_rejoin++;
			  }
			  else {
				  System.out.println("shed failed, exit leave and rejoin");
				  return;
			  }

			  message_num++;
			  agreed = false;
		  }
		  else {
			  System.out.println("rejected by heavy node, exit this round");
			  //vector = null;
			  return;
		  }
		  
		  
		  String response = Helper.sendRequest(new_pre, "YOURID");
		  message_num++;
		  long pre_id = Long.parseLong(response);
		  String succ = Helper.sendRequest(new_pre, "YOURSUCC");
		  String suc_addr = succ.split("_")[1];
		  InetSocketAddress new_suc = Helper.createSocketAddress(suc_addr);
		  response = Helper.sendRequest(new_suc, "YOURID");
		  Long suc_id = Long.valueOf(response);
		  
		  System.out.println("\n\n\n");
		  System.out.println("the new predecessor is "+new_pre.getPort());
		 
		  System.out.println("\n\n\n");
		  
		  System.out.println("the old id is: "+localId);
		  
		  long M = Helper.powerOfTwo.get(32);
		  Long targetId;
		  if(pre_id<suc_id) {//normal situation
			  Long range = suc_id-pre_id+1;
			  Long shift = (long) (Math.random()*range);
			  targetId = pre_id + 1 + shift;
		  }
		  else {//cross the zero point
			  Long range = M - suc_id + pre_id + 1;
			  Long shift = (long) (Math.random()*(range));
			  targetId = (pre_id + 1 + shift) % M;
		  }
		   

		  System.out.println("the new id is: "+targetId);
		  System.out.println("\n\n\n");
			  
		  id_modified = true;
		  
		  stabilize.toDie();
		  fix_fingers.toDie();
		  ask_predecessor.toDie();
		  
		  long message_id = System.currentTimeMillis();
			String ip = localAddress.getAddress().toString().split("/")[1];
			String port_s = String.valueOf(localAddress.getPort());
			String address = ip+":"+port_s;
			//Helper.sendRequest(this.localAddress, "CLEARME_"+address+"_"+message_id);
			for (int i = 1; i <= 32; i++) {
				//if(finger.get(i)!=null)
					//Helper.sendRequest(finger.get(i), "CLEARME_"+address+"_"+message_id);
				updateIthFinger (i, null);
			}

			//Helper.sendRequest(predecessor, "CLEARME_"+address+"_"+message_id);
			
			/*try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    */
		  predecessor = null;
		  finger = new HashMap<Integer, InetSocketAddress>();
		  for(int i=1;i<=32;i++) {
			  updateIthFinger(i,null);
		  }
	      

		  stabilize = new Stabilize(this,true);
		  fix_fingers = new FixFingers(this);
		  ask_predecessor = new AskPredecessor(this);
		  
		  if (contact_node != null && !contact_node.equals(localAddress)) {
				System.out.println("send request to find successor");
				InetSocketAddress successor = Helper.requestAddress(contact_node, "FINDSUCC_" + targetId);
				if(successor!=null)
					System.out.println("response successor is --- "
										+successor.getAddress().toString().split("/")[1]+":"+successor.getPort());
				else 
					System.out.println("response successor is null");
				if (successor == null)  {
					System.out.println("\nCannot find node you are trying to contact. Please exit.\n");
					return ;
				}
				System.out.println("update successor");
				updateIthFinger(1, successor);
			}
			setLocalId(targetId);
			// start all threads
			
			System.out.println("stablizer");
			stabilize.start();
			System.out.println("fix finger");
			fix_fingers.start();
			System.out.println("ask predecessor");
			ask_predecessor.start();
		  
		  System.out.println("exit leave and rejoin precess");
		  
	  }

private int find_k1(DN[] light_nodes, String id) {
		for(int i=0;i<light_nodes.length;i++) {
			if(light_nodes[i].id.equals(id)) return i+1;
		}
		return -1;
	}

public String compute_k2( DN[] heavy_nodes, long average, int k1) {
		String result = null;
		long sum_k1 = average * k1;
		int count_k1 = 0;
		int count_k2 = 0;
		if(heavy_nodes==null||heavy_nodes.length==0) return null;
		for(int i=0; i < heavy_nodes.length; i++) {
			count_k2++;
			long diff = heavy_nodes[i].used - average;
			for(;diff > 0.2*average;) {
				count_k1++;
				long move_size = 0;
				if(diff > average)  move_size = average;
				else move_size = diff;
				diff -= average;
				if(count_k1 == k1) return String.valueOf(count_k2)+":"+String.valueOf(move_size);
				
			}
			
		}
		//new Exception().printStackTrace();
		return null;
	}

//--------------------------------------------------------------------------------------------------------------------------------
	/*
	 *   public String ipAddr;     // IP address
		 *	 public String hostName;   // hostname claimed by datanode
		 *	 public String peerHostName; // hostname from the actual connection
		 *	 public int xferPort;      // data streaming port
		 *	 public int infoPort;      // info server port
		 *	 public int infoSecurePort; // info server port
		 *	 public int ipcPort;       // IPC server port
		 *	 public String xferAddr;
	 *	 public final String datanodeUuid;
	 */
	
	/*
	public DatanodeInfo(DatanodeID nodeID, String location,
		      final long capacity, final long dfsUsed, final long remaining,
		      final long blockPoolUsed, final long cacheCapacity, final long cacheUsed,
		      final long lastUpdate, final long lastUpdateMonotonic,
		      final int xceiverCount, final AdminStates adminState,
		      final String upgradeDomain)
	*/

/*
public String toString() {
	String result = version + "@";
	int i=1;
	for(Entry<String,String> entry: table.entrySet()) {
		String id = entry.getKey().toString();
		String nodeinfo = entry.getValue();
		if(i==1)result = result + id + " " + nodeinfo;
		else result = result + "," + id + " " + nodeinfo;
		i++;
		
	}
	return result;
	
}
*/
	public void initializeVector() throws IOException {
		vector = new Vector();
		String nodeinfo = this.getCapacity() + " " 
						+ this.getDFSUsed() + " " 
						+ dn.id.ipAddr + " "
						+ dn.id.hostName + " "
						+ dn.id.peerHostName + " "
						+ dn.id.xferPort + " "
						+ dn.id.infoPort + " "
						+ dn.id.infoSecurePort + " "
						+ dn.id.ipcPort + " "
						+ dn.id.xferAddr + " "
						+ dn.id.datanodeUuid + " "
						+ NetworkTopology.DEFAULT_RACK + " "//location
						+ dn.getFSDataset().getRemaining() + " "
						//getblockPoolUsed
						+ dn.getFSDataset().getCacheCapacity() + " "
						+ dn.getFSDataset().getCacheUsed() + " "
						+ localAddress.getPort()
						//lastUpdate
						//lastUpdateMonotonic
						//xceiverCount
						//AdminStates
						//upgradeDomain
						;
		vector.table.put(dn.getDatanodeUuid(), nodeinfo);
	}
	
	public void randomlyselect() {
		
		int num = 5;
		for(int i=1;i<=finger.size() && vector.table.size()<num;i++) {
			int index = (int)(Math.random()*31)+1;
			if(finger.get(index) == null) continue;
			InetSocketAddress node = finger.get(index);
			String dnUid = Helper.sendRequest(node, "YOURUUID");
			this.message_num++;
			if(vector.table.containsKey(dnUid)) continue;
			String vec_s = Helper.sendRequest(node, "YOURVECTOR rand_select");
			this.message_num++;
			Vector other;
			if(vec_s == null || vec_s.startsWith("INITIALIZING")) {
				if(finger.get(index)!=null)
					System.out.println("the node "+finger.get(index).getHostName()+" is still initializing its vector");
				else
					System.out.println("finger is updating");
				continue;
			}
			else {
				System.out.println("node is "+finger.get(index).getHostName());
				other = build(vec_s);
				System.out.println(other.table.get(dnUid));
			}
			String nodeinfo = other.table.get(dnUid);
			vector.table.put(dnUid, nodeinfo);
		}
		
	}
	
	
	public void updateVector(Vector other){
		//if(other.version<vector.version)
		//	return;
		if(other == null) {
			System.out.println("vector is null in updatevector,exit");
			return;
		}
		boolean updated = false;
		for(Entry<String, String> entry: other.table.entrySet()) {
			String id = entry.getKey();
			String nodeinfo = entry.getValue();
			
			if(vector.table.containsKey(id)) {
				String myinfo = vector.table.get(id);
				if(!myinfo.equals(nodeinfo) && !id.equals(dn.getDatanodeUuid())) {
					
					String ip = nodeinfo.split(" ")[2];
					String port = nodeinfo.split(" ")[15];

					Long usage = Long.valueOf(nodeinfo.split(" ")[1]);
					InetSocketAddress target =  Helper.createSocketAddress(ip+":"+port);
					String cur_usage_s = Helper.sendRequest(target, "YOURDFSUSED");
					long cur_usage = Long.parseLong(cur_usage_s);
					if(usage == cur_usage) {
						vector.table.put(id, nodeinfo);
					}
					else {
						
						String[] parts = nodeinfo.split(" ");
						parts[1] = String.valueOf(cur_usage_s);
						nodeinfo = "";
						int i = 1;
						for(String s : parts) {
							if(i==1)
								nodeinfo += s;
							else
								nodeinfo += " "+s;
							i++;
						}
						vector.table.put(id, nodeinfo);
					}
				}
			}
			
			else {
				String ip = nodeinfo.split(" ")[2];
				String port = nodeinfo.split(" ")[15];

				Long usage = Long.valueOf(nodeinfo.split(" ")[1]);
				InetSocketAddress target =  Helper.createSocketAddress(ip+":"+port);
				String cur_usage_s = Helper.sendRequest(target, "YOURDFSUSED");
				long cur_usage = Long.parseLong(cur_usage_s);
				if(usage == cur_usage) {
					if(vector.table.size()<vector.MAX_NUM)
						vector.add(id, nodeinfo);
				}
				else {
					String[] parts = nodeinfo.split(" ");
					parts[1] = String.valueOf(cur_usage_s);
					nodeinfo = "";
					int i = 1;
					for(String s : parts) {
						if(i==1)
							nodeinfo += s;
						else
							nodeinfo += " "+s;
						i++;
					}
					if(vector.table.size()<vector.MAX_NUM)
						vector.add(id, nodeinfo);
				}
				
			}
			
		}
		
		if(updated) vector.version = other.version;
	}
	
	public Vector build(String vector) {
		if(vector == null) {
			System.out.println("vector is null in build, exit");
			return null;
		}
		String[] division = vector.split("@");
		int version = Integer.parseInt(division[0]);
		String table = division[1];
		String[] items = table.split(",");
		Map<String, String> t = new HashMap<String, String>();
		for(String item: items) {
			String[] partition = item.split(" ");
			String id = partition[0];
			String nodeinfo = "";
			for(int i=1; i<partition.length;i++) {
				if(i==1) nodeinfo += partition[i];
				else nodeinfo += " "+partition[i];				
			}
			t.put(id, nodeinfo);
		}
		Vector other = new Vector(version, t);
		return other;
		
	}
	
	public long getAverage() {
		int num = vector.table.size();
		long dfsused_sum = 0L;

		for(String nodeinfo : vector.table.values()) {
			String[] partition = nodeinfo.split(" ");
			dfsused_sum += Long.parseLong(partition[1]);
		}
		return (long)(dfsused_sum/num);
	}
	
	public void exchangeVector() {
		
		boolean f_pre = true;
		boolean f_succ = true;
		String pre_vec;
		String suc_vec;
		int i=0;
		do{
			/*while(predecessor==null)
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}*/
			//synchronized(predecessor) {
			while(predecessor==null) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
				pre_vec = Helper.sendRequest(predecessor, "YOURVECTOR exc_vec");
			//}
			message_num++;
			if(pre_vec != null)
				if(!pre_vec.startsWith("INITIALIZING"))
					f_pre = false;
			i++;
			try {
				Thread.sleep(30);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}while(f_pre&&i<=5);
		
		int j=0;
		
		do{
			/*while(finger.get(1)==null) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}*/
			//synchronized(finger.get(1)) {
			System.out.println("successor updating");
			while(finger.get(1)==null) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			System.out.println("update successor done");
			suc_vec = Helper.sendRequest(finger.get(1), "YOURVECTOR exc_vec");
			//}
			message_num++;
			if(suc_vec != null)
				if(!suc_vec.startsWith("INITIALIZING"))
					f_succ = false;
			j++;
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}while(f_succ&&j<=5);
		 
		
		if(!f_pre) { 
			Vector pv = build(pre_vec);
			updateVector(pv);
		}
		else {
			
		}
			
		if(!f_succ) {
			Vector sv = build(suc_vec);
			updateVector(sv);
		}
		else {
			
		}
		vector.version++;
	}
	
	public boolean gossipProtocal() throws InterruptedException {
		for(int i = 0 ;vector.table.size()<vector.MAX_NUM && i<10;i++) {
			exchangeVector();
			Thread.sleep(600);
		}
		//if(vector.table.size()<vector.MAX_NUM) {
		//	return false;
		//}
		//else {
		//	return true;
		//}
		return true;
	}
	public void printvector() {
		System.out.println(vector.toString());
	}
	
	/*
	public DatanodeInfo(DatanodeID nodeID) {
	    super(nodeID);
	    this.capacity = 0L;
	    this.dfsUsed = 0L;
	    this.nonDfsUsed = 0L;
	    this.remaining = 0L;
	    this.blockPoolUsed = 0L;
	    this.cacheCapacity = 0L;
	    this.cacheUsed = 0L;
	    this.lastUpdate = 0L;
	    this.lastUpdateMonotonic = 0L;
	    this.xceiverCount = 0;
	    this.adminState = null;
	  }
	*/
	
	/*
	String nodeinfo = this.getCapacity() + " " 0
						+ this.getDFSUsed() + " " 1
						+ dn.id.ipAddr + " " 2
						+ dn.id.hostName + " " 3
						+ dn.id.peerHostName + " " 4
						+ dn.id.xferPort + " " 5
						+ dn.id.infoPort + " " 6
						+ dn.id.infoSecurePort + " " 7 
						+ dn.id.ipcPort + " " 8
						+ dn.id.xferAddr + " " 9
						+ dn.id.datanodeUuid 10
						+ NetworkTopology.DEFAULT_RACK//location 11
						+ dn.getFSDataset().getRemaining() 12
						//getblockPoolUsed
						+ dn.getFSDataset().getCacheCapacity() 13
						+ dn.getFSDataset().getCacheUsed() 14
						//lastUpdate
						//lastUpdateMonotonic
						//xceiverCount
						//AdminStates
						//upgradeDomain
						;
	*/
	
	public DatanodeInfo build_NodeInfo(String id, Vector other) {
		String[] nodeinfos ;
		if(!vector.table.containsKey(id)) {
			nodeinfos = other.table.get(id).split(" ");			
		}
		
		else {
			nodeinfos = vector.table.get(id).split(" ");
		}
		
		long capacity = Long.parseLong(nodeinfos[0]);
		long dfsUsed = Long.parseLong(nodeinfos[1]);
		String ipAddr = nodeinfos[2];
		String hostName = nodeinfos[3];
		String peerHostName = nodeinfos[4];
		int xferPort = Integer.parseInt(nodeinfos[5]);
		int infoPort = Integer.parseInt(nodeinfos[6]);
		int infoSecurePort = Integer.parseInt(nodeinfos[7]);
		int ipcPort = Integer.parseInt(nodeinfos[8]);
		String xferAddr = nodeinfos[9];
		String datanodeUuid = nodeinfos[10];
		String location = nodeinfos[11];
		long remaining = Long.parseLong(nodeinfos[12]);
		long blockPoolUsed = 0L;
		long CacheCapacity = Long.parseLong(nodeinfos[13]);
		long CacheUsed = Long.parseLong(nodeinfos[14]);
		long lastUpdate = 0L;
		long lastUpdateMonotonic = 0L;
		AdminStates adminStates = AdminStates.NORMAL;
		int xceiverCount = 0;
		String upgradeDomain = "";
		//public DatanodeID(String ipAddr, String hostName, String datanodeUuid,
		//	      int xferPort, int infoPort, int infoSecurePort, int ipcPort)
		
		DatanodeID datanodeid =  new DatanodeID(ipAddr, hostName, datanodeUuid, xferPort, infoPort, infoSecurePort, ipcPort);
		
		/*
		 * public DatanodeInfo(DatanodeID nodeID, String location,
      						   final long capacity, final long dfsUsed, final long remaining,
      						   final long blockPoolUsed, final long cacheCapacity, final long cacheUsed,
      						   final long lastUpdate, final long lastUpdateMonotonic,
      						   final int xceiverCount, final AdminStates adminState,
      						   final String upgradeDomain)
		 */
		
		return new DatanodeInfo(datanodeid, location, capacity, dfsUsed, remaining, blockPoolUsed, CacheCapacity,
								CacheUsed, lastUpdate, lastUpdateMonotonic, xceiverCount, adminStates, upgradeDomain);
	
	
	}
	
//--------------------------------------------------------------------------------------------------------------------------------	
	public void setLocalId(long new_id) {
		localId = new_id;
		id_modified = true;
	}
	
	public long getCapacity() {
		return dn.getFSDataset().getCapacity();
	}
	
	public long getDFSUsed() throws IOException {
		return dn.getFSDataset().getDfsUsed();
	}
	
	public long getOtherId(InetSocketAddress server, String req) {
		String response = Helper.sendRequest(server, "YOURID");
		message_num++;
		return Long.parseLong(response);
	}
	
	public long getOtherCapacity(InetSocketAddress server, String req) {
		String response = Helper.sendRequest(server, "YOURCAP");
		message_num++;
		return Long.parseLong(response);
	}
	
	public long getOtherDFSUsed(InetSocketAddress server, String req) {
		String response = Helper.sendRequest(server, "YOURDFSUSED");
		message_num++;
		return Long.parseLong(response);
	}
	/*
	public Node (InetSocketAddress address,DataNode _dn, InetSocketAddress new_pre) {
		dn = _dn;
		localAddress = address;
		
		String response = Helper.sendRequest(new_pre, "YOURID");
		long pre_id = Long.parseLong(response);
		
		InetSocketAddress new_succ = find_successor(pre_id);
		response = Helper.sendRequest(new_succ, "YOURID");
		long succ_id = Long.parseLong(response);
		
		localId = (pre_id + succ_id) / 2;
		
		predecessor = new_pre;
		updateIthFinger(1,new_succ);
		
		// initialize the rest of items in finger table
		finger = new HashMap<Integer, InetSocketAddress>();
		for (int i = 2; i <= 32; i++) {
			updateIthFinger (i, null);
		}
		request_nodes = new HashMap<InetSocketAddress,Long>();
		// initialize threads
		listener = new Listener(this);
		stabilize = new Stabilize(this);
		fix_fingers = new FixFingers(this);
		ask_predecessor = new AskPredecessor(this);
		sender = new Sender(this);
	}
	*/
	
	public Node (InetSocketAddress address,DataNode _dn,InetSocketAddress contact_node) {
		trans_transferring_blk_num = 0;
		shed_transferring_blk_num = 0;
		trans_terminate = false;
		shed_transferring = false;
		shed_receiving = false;
		trans_transferring = false;
		trans_receiving = false;
		
		shed_blkid = null;
		trans_blkid = null;
		
		id_modified_server = new HashMap<String,Long>();
		id_server = new HashMap<String,Long>();
		
		transfer = false;
		id_modified = false;

		trans_new_succ = null;
		new_pred = null;
		shed_old_pre = null;
		shed_old_succ = null;
		
		request_nodes = new HashMap<InetSocketAddress,Long>();
		
		waitforrequest = false;
		getResponse = false;//response from heavy node for share load request 
		
		//balancing = true;// already in balancing status or not begin balancing
		
		agreed = false;// be chosen from heavy node
		
		System.out.println("creating node");
		this.contact_node = contact_node;
		new_pred = null;
		shed_old_pre = null;
		dn = _dn;
		localAddress = address;
		localId = Helper.hashSocketAddress(localAddress);

		// initialize an empty finger table
		finger = new HashMap<Integer, InetSocketAddress>();
		for (int i = 1; i <= 32; i++) {
			updateIthFinger (i, null);
		}

		// initialize predecessor
		predecessor = null;
		request_nodes = new HashMap<InetSocketAddress,Long>();
		// initialize threads
		listener = new Listener(this);
		stabilize = new Stabilize(this,false);
		fix_fingers = new FixFingers(this);
		ask_predecessor = new AskPredecessor(this);

	}

	/**
	 * Create or join a ring 
	 * @param contact
	 * @return true if successfully create a ring
	 * or join a ring via contact
	 */
	public boolean join (InetSocketAddress contact) {

		// if contact is other node (join ring), try to contact that node
		// (contact will never be null)
		System.out.println("begin joining in the chord, contact node--- "+
							contact.getAddress().toString().split("/")[1]+":"+contact.getPort());
		
		if (contact != null && !contact.equals(localAddress)) {
			System.out.println("send request to find successor");
			InetSocketAddress successor = Helper.requestAddress(contact, "FINDSUCC_" + localId);
			if(successor!=null)
				System.out.println("response successor is --- "
									+successor.getAddress().toString().split("/")[1]+":"+successor.getPort());
			else 
				System.out.println("response successor is null");
			if (successor == null)  {
				System.out.println("\nCannot find node you are trying to contact. Please exit.\n");
				return false;
			}
			System.out.println("update successor");
			updateIthFinger(1, successor);
		}

		// start all threads
		System.out.println("listener");
		listener.start();
		System.out.println("stablizer");
		stabilize.start();
		System.out.println("fix finger");
		fix_fingers.start();
		System.out.println("ask predecessor");
		ask_predecessor.start();
		initializing = true;
		return true;
	}

	/**
	 * Notify successor that this node should be its predecessor
	 * @param successor
	 * @return successor's response
	 */
	public String notify(InetSocketAddress successor) {
		if (successor!=null && !successor.equals(localAddress)) {
			//System.out.println("notify its successor " + successor.getAddress().toString().split("/")[1]+":"+successor.getPort());
			if(balancing) message_num++;
			return Helper.sendRequest(successor, "IAMPRE_"+localAddress.getAddress().toString().split("/")[1]+":"+localAddress.getPort()+"_"+localId);
		}
		else
			return null;
	}

	/**
	 * Being notified by another node, set it as my predecessor if it is.
	 * @param newpre
	 */
	
	public long computeId(InetSocketAddress server) {
		/*
		StackTraceElement stack[] = new Throwable().getStackTrace();
		  for(int i=0;i<stack.length;i++) {
			  StackTraceElement ste = stack[i];
			  System.out.println(ste.getClassName()+"."+ste.getMethodName()+"(...)");
			  System.out.println(i+" -- method name "+ste.getMethodName());
			  System.out.println(i+" -- filename "+ste.getFileName());
			  System.out.println(i+" -- line number "+ste.getLineNumber());
		  }
		  */
		if(server.equals(localAddress)) {
			if(id_modified) {
				return localId;
			}
			else {
				return Helper.hashSocketAddress(localAddress);
			}
			
		}
		
		String address = server.getAddress().toString().split("/")[1]+":"+server.getPort();	
		String id_mod = Helper.sendRequest(server, "IDMOD");
		
		long id = 0L;
		if(id_mod.equals("N")) {
			id = Helper.hashSocketAddress(server);
		}
		else {
			String id_s = Helper.sendRequest(server, "YOURID");
			id = Long.parseLong(id_s);
		}
		return id;
	}
	
	public void notified (InetSocketAddress newpre,long id) {
		//System.out.println("being notified by its predecessor " + newpre.getAddress().toString().split("/")[1]+":"+newpre.getPort());
		if (predecessor == null || predecessor.equals(localAddress)) {
			this.setPredecessor(newpre);
		}
		else {
			
			long oldpre_id = computeId(predecessor);
			long local_relative_id = Helper.computeRelativeId(localId, oldpre_id);
			long newpre_relative_id = Helper.computeRelativeId(id, oldpre_id);
			if (newpre_relative_id > 0 && newpre_relative_id < local_relative_id)
				this.setPredecessor(newpre);
		}
	}

	/**
	 * Ask current node to find id's successor.
	 * @param id
	 * @return id's successor's socket address
	 */
	public InetSocketAddress find_successor (long id) {

		// initialize return value as this node's successor (might be null)
		InetSocketAddress ret = this.getSuccessor();

		// find predecessor
		InetSocketAddress pre = find_predecessor(id);

		// if other node found, ask it for its successor
		if (!pre.equals(localAddress))
			ret = Helper.requestAddress(pre, "YOURSUCC");

		// if ret is still null, set it as local node, return
		if (ret == null)
			ret = localAddress;

		return ret;
	}

	/**
	 * Ask current node to find id's predecessor
	 * @param id
	 * @return id's successor's socket address
	 */
	private InetSocketAddress find_predecessor (long findid) {
		InetSocketAddress n = this.localAddress;
		InetSocketAddress n_successor = this.getSuccessor();
		InetSocketAddress most_recently_alive = this.localAddress;
		
		
		
		long n_successor_relative_id = 0;
		if (n_successor != null)
			n_successor_relative_id = Helper.computeRelativeId(computeId(n_successor), computeId(n));
		long findid_relative_id = Helper.computeRelativeId(findid, computeId(n));

		while (!(findid_relative_id > 0 && findid_relative_id <= n_successor_relative_id)) {

			// temporarily save current node
			InetSocketAddress pre_n = n;

			// if current node is local node, find my closest
			if (n.equals(this.localAddress)) {
				n = this.closest_preceding_finger(findid);
			}

			// else current node is remote node, sent request to it for its closest
			else {
				InetSocketAddress result = Helper.requestAddress(n, "CLOSEST_" + findid);

				// if fail to get response, set n to most recently 
				if (result == null) {
					n = most_recently_alive;
					n_successor = Helper.requestAddress(n, "YOURSUCC");
					if (n_successor==null) {
						System.out.println("It's not possible.");
						return localAddress;
					}
					continue;
				}

				// if n's closest is itself, return n
				else if (result.equals(n))
					return result;

				// else n's closest is other node "result"
				else {	
					// set n as most recently alive
					most_recently_alive = n;		
					// ask "result" for its successor
					n_successor = Helper.requestAddress(result, "YOURSUCC");	
					// if we can get its response, then "result" must be our next n
					if (n_successor!=null) {
						n = result;
					}
					// else n sticks, ask n's successor
					else {
						n_successor = Helper.requestAddress(n, "YOURSUCC");
					}
				}

				// compute relative ids for while loop judgement
				n_successor_relative_id = Helper.computeRelativeId(computeId(n_successor),computeId(n));
				findid_relative_id = Helper.computeRelativeId(findid, computeId(n));
			}
			if (pre_n.equals(n))
				break;
		}
		return n;
	}

	/**
	 * Return closest finger preceding node.
	 * @param findid
	 * @return closest finger preceding node's socket address
	 */
	public InetSocketAddress closest_preceding_finger (long findid) {
		long findid_relative = Helper.computeRelativeId(findid, localId);

		// check from last item in finger table
		for (int i = 32; i > 0; i--) {
			InetSocketAddress ith_finger = finger.get(i);
			if (ith_finger == null) {
				continue;
			}
			long ith_finger_id = computeId(ith_finger);
			long ith_finger_relative_id = Helper.computeRelativeId(ith_finger_id, localId);

			// if its relative id is the closest, check if its alive
			if (ith_finger_relative_id > 0 && ith_finger_relative_id < findid_relative)  {
				String response = "";
				try {
					response = Helper.sendRequest(ith_finger, "KEEP");
				}
				catch(Exception e) {
					System.out.println("server is "+ith_finger.getAddress().toString()+":"+ith_finger.getPort());
				}
				//it is alive, return it
				if (response!=null &&  response.equals("ALIVE")) {
					return ith_finger;
				}

				// else, remove its existence from finger table
				else {
					updateFingers(-2, ith_finger);
				}
			}
		}
		return localAddress;
	}

	/**
	 * Update the finger table based on parameters.
	 * Synchronize, all threads trying to modify 
	 * finger table only through this method. 
	 * @param i: index or command code
	 * @param value
	 */
	public synchronized void updateFingers(int i, InetSocketAddress value) {

		// valid index in [1, 32], just update the ith finger
		if (i > 0 && i <= 32) {
			updateIthFinger(i, value);
		}

		// caller wants to delete
		else if (i == -1) {
			deleteSuccessor();
		}

		// caller wants to delete a finger in table
		else if (i == -2) {
			deleteCertainFinger(value);

		}

		// caller wants to fill successor
		else if (i == -3) {
			fillSuccessor();
		}

	}

	/**
	 * Update ith finger in finger table using new value
	 * @param i: index
	 * @param value
	 */
	public void updateIthFinger(int i, InetSocketAddress value) {
		finger.put(i, value);

		// if the updated one is successor, notify the new successor
		if (i == 1 && value != null && !value.equals(localAddress)) {
			notify(value);
		}
	}

	/**
	 * Delete successor and all following fingers equal to successor
	 */
	private void deleteSuccessor() {
		InetSocketAddress successor = getSuccessor();

		//nothing to delete, just return
		if (successor == null)
			return;

		// find the last existence of successor in the finger table
		int i = 32;
		for (i = 32; i > 0; i--) {
			InetSocketAddress ithfinger = finger.get(i);
			if (ithfinger != null && ithfinger.equals(successor))
				break;
		}

		// delete it, from the last existence to the first one
		for (int j = i; j >= 1 ; j--) {
			updateIthFinger(j, null);
		}

		// if predecessor is successor, delete it
		if (predecessor!= null && predecessor.equals(successor))
			setPredecessor(null);

		// try to fill successor
		fillSuccessor();
		successor = getSuccessor();

		// if successor is still null or local node, 
		// and the predecessor is another node, keep asking 
		// it's predecessor until find local node's new successor
		if ((successor == null || successor.equals(successor)) && predecessor!=null && !predecessor.equals(localAddress)) {
			InetSocketAddress p = predecessor;
			InetSocketAddress p_pre = null;
			while (true) {
				p_pre = Helper.requestAddress(p, "YOURPRE");
				if (p_pre == null)
					break;

				// if p's predecessor is node is just deleted, 
				// or itself (nothing found in p), or local address,
				// p is current node's new successor, break
				if (p_pre.equals(p) || p_pre.equals(localAddress)|| p_pre.equals(successor)) {
					break;
				}

				// else, keep asking
				else {
					p = p_pre;
				}
			}

			// update successor
			updateIthFinger(1, p);
		}
	}

	/**
	 * Delete a node from the finger table, here "delete" means deleting all existence of this node 
	 * @param f
	 */
	private void deleteCertainFinger(InetSocketAddress f) {
		for (int i = 32; i > 0; i--) {
			InetSocketAddress ithfinger = finger.get(i);
			if (ithfinger != null && ithfinger.equals(f))
				finger.put(i, null);
		}
	}

	/**
	 * Try to fill successor with candidates in finger table or even predecessor
	 */
	private void fillSuccessor() {
		InetSocketAddress successor = this.getSuccessor();
		if (successor == null || successor.equals(localAddress)) {
			for (int i = 2; i <= 32; i++) {
				InetSocketAddress ithfinger = finger.get(i);
				if (ithfinger!=null && !ithfinger.equals(localAddress)) {
					for (int j = i-1; j >=1; j--) {
						updateIthFinger(j, ithfinger);
					}
					break;
				}
			}
		}
		successor = getSuccessor();
		if ((successor == null || successor.equals(localAddress)) && predecessor!=null && !predecessor.equals(localAddress)) {
			updateIthFinger(1, predecessor);
		}

	}


	/**
	 * Clear predecessor.
	 */
	public void clearPredecessor () {
		setPredecessor(null);
	}

	/**
	 * Set predecessor using a new value.
	 * @param pre
	 */
	private synchronized void setPredecessor(InetSocketAddress pre) {
		predecessor = pre;
	}


	/**
	 * Getters
	 * @return the variable caller wants
	 */

	public long getId() {
		return localId;
	}

	public InetSocketAddress getAddress() {
		return localAddress;
	}

	public InetSocketAddress getPredecessor() {
		return predecessor;
	}

	public InetSocketAddress getSuccessor() {
		InetSocketAddress a = null;
		if (finger != null && finger.size() > 0) {
			/*while(finger.get(1)==null) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}*/
			//synchronized(finger.get(1)) {
				a = finger.get(1);
			//}
			return a;
		}
		return null;
	}

	/**
	 * Print functions
	 */

	public void printNeighbors () {
		System.out.println("\nYou are listening on port "+localAddress.getPort()+"."
				+ "\nYour position is "+Helper.hexIdAndPosition(localAddress)+".");
		InetSocketAddress successor = finger.get(1);
		
		// if it cannot find both predecessor and successor
		if ((predecessor == null || predecessor.equals(localAddress)) && (successor == null || successor.equals(localAddress))) {
			System.out.println("Your predecessor is yourself.");
			System.out.println("Your successor is yourself.");

		}
		
		// else, it can find either predecessor or successor
		else {
			if (predecessor != null) {
				System.out.println("Your predecessor is node "+predecessor.getAddress().toString().split("/")[1]+", "
						+ "port "+predecessor.getPort()+ ", position "+Helper.hexIdAndPosition(predecessor)+".");
			}
			else {
				System.out.println("Your predecessor is updating.");
			}

			if (successor != null) {
				System.out.println("Your successor is node "+successor.getAddress().toString().split("/")[1]+", "
						+ "port "+successor.getPort()+ ", position "+Helper.hexIdAndPosition(successor)+".");
			}
			else {
				System.out.println("Your successor is updating.");
			}
		}
	}

	public void printDataStructure () {
		System.out.println("\n==============================================================");
		System.out.println("\nLOCAL:\t\t\t\t"+localAddress.toString()+"\t"+Helper.hexIdAndPosition(localAddress));
		if (predecessor != null)
			System.out.println("\nPREDECESSOR:\t\t\t"+predecessor.toString()+"\t"+Helper.hexIdAndPosition(predecessor));
		else 
			System.out.println("\nPREDECESSOR:\t\t\tNULL");
		System.out.println("\nFINGER TABLE:\n");
		for (int i = 1; i <= 32; i++) {
			long ithstart = Helper.ithStart(localId,i);
			InetSocketAddress f = finger.get(i);
			StringBuilder sb = new StringBuilder();
			sb.append(i+"\t"+ Helper.longTo8DigitHex(ithstart)+"\t\t");
			if (f!= null)
				sb.append(f.toString()+"\t"+Helper.hexIdAndPosition(f));

			else 
				sb.append("NULL");
			System.out.println(sb.toString());
		}
		System.out.println("\n==============================================================\n");
	}

	/**
	 * Stop this node's all threads.
	 */
	public void stopAllThreads() {
		if (listener != null)
			listener.toDie();
		if (fix_fingers != null)
			fix_fingers.toDie();
		if (stabilize != null)
			stabilize.toDie();
		if (ask_predecessor != null)
			ask_predecessor.toDie();
	}
}
