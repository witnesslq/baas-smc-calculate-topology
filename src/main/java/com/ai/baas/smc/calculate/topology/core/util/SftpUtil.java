package com.ai.baas.smc.calculate.topology.core.util;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class SftpUtil {

	private ChannelSftp sftp = null;
	private Session session = null;
	private Channel channel = null;
	private String user = "";
	private String pwd = "";
	private String host = "";
	private int port  = 22;
	
	public SftpUtil(String user,String pwd,String host,String port){
		this.user = user;
		this.pwd = pwd;
		this.host = host;
		if(!StringUtils.isBlank(port)){
			this.port = Integer.parseInt(port);
		}
	}
	
	public boolean connect() throws JSchException{
    	JSch jsch = new JSch();
		jsch.getSession(user, host, port);
		session = jsch.getSession(user, host, port);
        session.setPassword(pwd);
        Properties sshConfig = new Properties();
        sshConfig.put("StrictHostKeyChecking", "no");
        session.setConfig(sshConfig);
        session.connect();
        channel = session.openChannel("sftp");
        channel.connect();
        sftp = (ChannelSftp) channel;
        System.out.println("Connected to " + host + " success");
		return true;
	}
	
	
	public void uploadFile(String local,String remoteDir)throws Exception{
		try {
			sftp.cd(remoteDir);
		} catch (SftpException sftpException) {
			 if (ChannelSftp.SSH_FX_NO_SUCH_FILE == sftpException.id) {
				try {
					makeDir(remoteDir, sftp);
					sftp.cd(remoteDir);
				} catch (SftpException e) {
					e.printStackTrace();
				}
             }
		}
		File fileUploadZip = new File(local);
		InputStream is = null;
		try {
			is = new FileInputStream(fileUploadZip);
			sftp.put(is, fileUploadZip.getName());
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally{
			if(is != null){
				is.close();
			}
		}
	}
	
	
	private void makeDir(String directory, ChannelSftp sftp) throws SftpException {
        System.out.println(directory);
        System.out.println(sftp.pwd());
        String parentPath = new File(directory).getParentFile().getPath().replace("\\", "/");
        if (parentPath.equals("/")) {
            sftp.mkdir(directory.substring(1));
        } else {
            try {
                sftp.cd(parentPath);
            } catch (SftpException sException) {
                if (ChannelSftp.SSH_FX_NO_SUCH_FILE == sException.id) {
                    makeDir(parentPath, sftp);
                }
            }
            sftp.mkdir(directory);
        }
    }
	
	public void disconnect(){
		sftp.disconnect();
        if (session != null) {
            if (session.isConnected()) {
                session.disconnect();
            }
        }
        if (channel != null) {
            if (channel.isConnected()) {
                channel.disconnect();
            }
        }
	}
	
}
