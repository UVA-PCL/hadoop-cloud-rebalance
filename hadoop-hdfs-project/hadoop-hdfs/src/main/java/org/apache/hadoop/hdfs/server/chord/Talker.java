package org.apache.hadoop.hdfs.server.chord;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportReplica;
import org.apache.hadoop.hdfs.server.datanode.BPOfferService;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;









/**
 * Talker thread that processes request accepted by listener and writes
 * response to socket.
 * @author Chuan Xia
 *
 */

public class Talker implements Runnable{

	Socket talkSocket;
	private Node local;

	public Talker(Socket _talkSocket, Node _local)
	{
		talkSocket = _talkSocket;
		local = _local;
	}

	public void run()
	{
		InputStream input = null;
		OutputStream output = null;
		try {
			input = talkSocket.getInputStream();
			String request = Helper.inputStreamToString(input);
			String response = processRequest(request);
			if (response != null) {
				output = talkSocket.getOutputStream();
				output.write(response.getBytes());
			}
			input.close();
		} catch (IOException e) {
			throw new RuntimeException(
					"Cannot talk.\nServer port: "+local.getAddress().getPort()+"; Talker port: "+talkSocket.getPort(), e);
		}
	}

	private String processRequest(String request) throws IOException
	{
		
		InetSocketAddress result = null;
		String ret = null;
		if (request  == null) {
			return null;
		}
		if (request.startsWith("CLOSEST")) {
			long id = Long.parseLong(request.split("_")[1]);
			result = local.closest_preceding_finger(id);
			String ip = result.getAddress().toString().split("/")[1];
			int port = result.getPort();
			ret = "MYCLOSEST_"+ip+":"+port;
			
		}
		
		
		
		else if(request.startsWith("YOURVECTOR")) {
			
			String[] info = request.split(" ");
			if(info[1].equals("trans_trans")) {
				ret = local.vector.toString();
				return ret;
			}
			
			if(info[1].equals("shed_trans")) {
				ret = local.vector.toString();
				return ret;
			}
			
			if(    local.shed_receiving 
				|| local.shed_transferring 
				|| local.trans_receiving 
				|| local.trans_transferring) {
				
				ret = "INITIALIZING";
				return ret;
			}
			
			if(local.initializing ){ 
				if(!local.waitforrequest 
				 && local.vector != null 
				 && local.vector.table.size() >= 1)
					ret = local.vector.toString();
				else
					ret = "INITIALIZING";		
			}
			else {
				ret = local.vector.toString();
				//System.out.println("ret = "+ret);
			}
		}
		else if(request.startsWith("YOURCAP")) {
			long capacity = local.getCapacity();
			ret = String.valueOf(capacity);
		}
		
		else if(request.startsWith("YOURDFSUSED")) {
			long DFSUsed = local.getDFSUsed();
			ret = String.valueOf(DFSUsed);
		}
		else if(request.startsWith("YOURID")) {
			long MyId = local.getId();
			ret = String.valueOf(MyId);
		}
		
		else if (request.startsWith("YOURSUCC")) {
			result = local.getSuccessor();
			if (result != null) {
				String ip = result.getAddress().toString().split("/")[1];
				int port = result.getPort();
				ret = "MYSUCC_"+ip+":"+port;
			}
			else {
				ret = "NOTHING";
			}
		}
		else if (request.startsWith("YOURPRE")) {
			result =local.getPredecessor();
			if (result != null) {
				String ip = result.getAddress().toString().split("/")[1];
				int port = result.getPort();
				ret = "MYPRE_"+ip+":"+port;
			}
			else {
				ret = "NOTHING";
			}
		}
		
		
		else if(request.startsWith("YOURUUID")) {
			 //return local.dn.storage.getDatanodeUuid();
			return local.dn.id.getDatanodeUuid();
		}
		else if(request.startsWith("SHARELOAD")) {
			String[] partition = request.split(",");
			long id = Long.parseLong(partition[2]);
			long size = Long.parseLong(partition[1]);
			
			String addr = partition[3];
			InetSocketAddress new_pre = Helper.createSocketAddress(addr);
			
			if(!local.transfer && !local.initializing && local.waitforrequest) {
				
				System.out.println("movement size = "+size+", request from "+talkSocket.getInetAddress().getHostName());
				local.request_nodes.put(new_pre, size);
				ret = "COPYTHAT";
			}
			else {
				//Helper.sendRequest(new_pre, "REJ");
				local.message_num++;
				ret = "INITIALIZING";
			}
			
			
		}
		
		else if(request.startsWith("ACC")) {
			local.getResponse = true;
			local.agreed = true;
		}
		else if(request.startsWith("REJ")) {
			local.getResponse = true;
			local.agreed = false;
		}
		
		else if(request.startsWith("READYTORECEIVE_TRANSFER")) {
			 System.out.println("light node finish shed_trnasferring");
			 local.dn.ready_to_receive = true;
		}
		
		else if(request.startsWith("TRANS_TERMINATE")) {
			System.out.println("trans terminate request from "+talkSocket.getInetAddress().getHostName());
			 local.trans_terminate = true;
			 local.trans_receiving = false;
			 local.trans_transferring = false;
			 local.shed_receiving = false;
			 local.shed_transferring = false;
		}
		
		else if(request.startsWith("WAKEUP")) {
			 local.balancing = false;
			 local.waitforrequest = false;
			 ret = "successfully finish balancing round";
		}
		
		else if(request.startsWith("TRANSFERBLOCK")) {
			 String[] blkinfo = request.split("_");
			 
			 int num_blk = blkinfo.length-1;
			 if(num_blk == 0) {
				 local.dn.receiving_blocks = new boolean[num_blk];
				 local.trans_blkid = new String[num_blk];
				 local.trans_receiving = false;
				 return "NO TRANSFER";
			 }
			 local.dn.receiving_blocks = new boolean[num_blk];
			  local.trans_blkid = new String[num_blk];
			  for(int i=0;i<num_blk;i++) {
				  local.dn.receiving_blocks[i] = true;
			  }
			  
			 for(int i=0;i<local.trans_blkid.length;i++)
			 local.trans_blkid[i] = blkinfo[i+1];
		}
		
		else if(request.startsWith("STOPWAITING")) {// I'm heavy node, receive stopwaiting message from light node
			//local.trans_receiving = false;
			local.trans_transferring = false;
			//local.shed_receiving = false;
			//local.shed_transferring = false;
			local.shed = false;
			local.waitshed = false;
			local.transfer = false;
		}
		
		else if(request.startsWith("READYTOSHED")) {
			
			String request_Uuid = request.split("_")[2];
			
			System.out.println("\nshed request from "+talkSocket.getInetAddress().getHostName());
			if(!local.shed) {
				System.out.println("shed is false, exit");
				ret = "INITIALIZING";
				return ret;
			}
			 if(//local.initializing ||
			    local.shed_receiving 
			 || local.shed_transferring 
			 || local.trans_receiving 
			 || local.trans_transferring) {
				 
				 if(!local.dn.ready_to_receive //I am heavy node, light node is not ready to receive
					&& local.trans_new_succ != null ) 
				 	{//heavy node wait for light node shed, 
					 //light node shed to successor 
					 //and successor is heavy node === wait for light node = request light node
					 
					 String wait_Uuid = Helper.sendRequest(local.trans_new_succ, "YOURUUID");
					 if(wait_Uuid.equals(request_Uuid)) {
						 //agree shed
					 }
					 else {
						 
						 //System.out.println("initializing:"+local.initializing);
						 System.out.println("shed_receiving:"+local.shed_receiving);
						 System.out.println("shed_transferring:"+local.shed_transferring);
						 System.out.println("trans_receiving:"+local.trans_receiving);
						 System.out.println("trans_transferring:"+local.trans_transferring);
						 System.out.println("ready_to_receive:"+local.dn.ready_to_receive);
						 System.out.println("request light node is:"+request_Uuid);
						 System.out.println("wait light node is:"+wait_Uuid);
						 if(local.trans_new_succ != null)
							 System.out.println("local.trans_new_succ :"+local.trans_new_succ.getHostName());
						 else
							 System.out.println("local.trans_new_succ is null");
						 
						 
						 ret = "INITIALIZING";
						 return ret;
					 }
					 
				 }
				 else {//no shed
					 
				   //System.out.println("initializing:"+local.initializing);
					 System.out.println("shed_receiving:"+local.shed_receiving);
					 System.out.println("shed_transferring:"+local.shed_transferring);
					 System.out.println("trans_receiving:"+local.trans_receiving);
					 System.out.println("trans_transferring:"+local.trans_transferring);
					 System.out.println("ready_to_receive:"+local.dn.ready_to_receive);
					 if(local.trans_new_succ != null)
						 System.out.println("local.trans_new_succ :"+local.trans_new_succ.getHostName());
					 else
						 System.out.println("local.trans_new_succ is null");
					 
					 
					 ret = "INITIALIZING";
					 return ret;
				 }
				 
			 }
			 
			
			 System.out.println("wait for predecessor update");
			 while(local.predecessor == null) {
				 try {
					Thread.sleep(150);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			 }
			 System.out.println("predecessor update done");
			 
			 String [] shedinfo = request.split("_");
			 String len = shedinfo[1];
			 int length = Integer.parseInt(len);
			 local.shed_old_pre = Helper.createSocketAddress(shedinfo[3]);
			 
			 if(length != 0) {
				 local.dn.shed_receiving = true;
				 local.shed_receiving = true;
				 local.shed_blkid = new String[length];
				 for(int i=0;i<length;i++) {
					 local.shed_blkid[i] = shedinfo[i+4];
				 }
				 local.dn.shed_receiving_blocks = new boolean[length];
				 for(int i=0;i<local.dn.shed_receiving_blocks.length;i++) {
					 local.dn.shed_receiving_blocks[i] = true;
				 }
			 }
			 
			 ret = "READY";
			 
		}
		
		else if(request.startsWith("TRANSTRANSFIN")) {
			 local.trans_receiving = false;
		}
		else if(request.startsWith("SHEDTRANSFIN")) {
			 System.out.println("finish shed_trans");
			 local.shed_receiving = false;
			 //local.dn.ready_to_receive = true;
			 //Helper.sendRequest(new_pre, "READYTORECEIVE_TRANSFER");
		}
		else if(request.startsWith("DELETE")) {
			
			String[] info = request.split("_");
			String PoolId = info[1];
			String BlockId = info[2];
			System.out.println("receive message from Talker port: "+talkSocket.getPort());
			System.out.println("receive message to delete the block: "+BlockId+", in block pool: "+PoolId);
			
			List<BPOfferService> Bpos_list = local.dn.getAllBpOs();
			for(BPOfferService bpos: Bpos_list) {
				Map<DatanodeStorage, BlockListAsLongs> perVolumeBlockLists = 
				local.dn.getFSDataset().getBlockReports(bpos.getBlockPoolId());
				String poolId = bpos.getBlockPoolId();
				for(Map.Entry<DatanodeStorage, BlockListAsLongs> entry: perVolumeBlockLists.entrySet()) {
					
					BlockListAsLongs bl = entry.getValue();
					
					for(BlockReportReplica b : bl) {
						if(b.getBlockId() != Long.parseLong(BlockId)) continue;
						try {
					        // using global fsdataset
							Block[] toDelete = {b};
							System.out.println("\n\nfind the block should be deleted\n\n");
					        local.dn.getFSDataset().invalidate(poolId, toDelete);
					      } catch(IOException e) {
					        // Exceptions caught here are not expected to be disk-related.
					        e.printStackTrace();
					      }
					}
				}
				
			}
			
			
		}
		else if(request.startsWith("BALANCING")) {
			
			if(!local.balancing) {
				ret ="begin balancing_stop_all_threads";
			local.balancing = true;
			local.waitforrequest = true;
			
			new Thread() {
				
				public void run(){
					try {
						local.loadbalance();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					finally {
						
					}
				}
			}.start();
			/*
			Helper.requestShare(local.predecessor, "BALANCING");
			for(int i=0;i<local.finger.size();i++) {
				Helper.requestShare(local.finger.get(i), "BALANCING");
			}
			*/
			}
			else ret ="already balancing";
			
			
		}
		else if (request.startsWith("FINDSUCC")) {
			long id = Long.parseLong(request.split("_")[1]);
			result = local.find_successor(id);
			String ip = result.getAddress().toString().split("/")[1];
			int port = result.getPort();
			ret = "FOUNDSUCC_"+ip+":"+port;
		}
		else if (request.startsWith("IAMPRE")) {
			InetSocketAddress new_pre = Helper.createSocketAddress(request.split("_")[1]);
			long new_pre_id = Long.parseLong(request.split("_")[2]);
			local.notified(new_pre,new_pre_id);
			ret = "NOTIFIED";
		}
		else if (request.startsWith("KEEP")) {
			if(!local.trans_receiving)
				ret = "ALIVE";
			else
				ret = "DIE";
		}
		else if (request.startsWith("IDMOD")) {
			if(local.id_modified) ret = "Y";
			else ret = "N";
		}
		
		return ret;
	}
}
