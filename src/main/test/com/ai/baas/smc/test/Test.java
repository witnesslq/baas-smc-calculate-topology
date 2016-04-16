package com.ai.baas.smc.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class Test {
	
	public static void main(String [] args) throws IOException
	{
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", "49181");
		configuration.set("hbase.zookeeper.quorum", "10.1.130.84,10.1.130.85,10.1.236.122");
		configuration.set("hbase.master", "10.1.130.84");
		String tableName="aaa";
		String[] familys=new String[1];
		familys[0]="bbbb";
		   HBaseAdmin admin = new HBaseAdmin(configuration);    
           if (admin.tableExists(tableName)) {    
               System.out.println("table already exists!");    
           } else {    
               HTableDescriptor tableDesc = new HTableDescriptor(tableName);    
               for(int i=0; i<familys.length; i++){    
                   tableDesc.addFamily(new HColumnDescriptor(familys[i]));    
               }    
               admin.createTable(tableDesc);    
               System.out.println("create table " + tableName + " ok.");    
           }      
		
	}

}
