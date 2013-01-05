package com.github.mergen.server;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ServerChannelFactory;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

// import com.github.nedis.pubsub.RedisListener;
import com.github.nedis.codec.*;

import com.hazelcast.config.Config;
import com.hazelcast.config.Join;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;

import com.beust.jcommander.JCommander;
import java.lang.Class;
import java.util.*;

public class Server {

	private final String host;
	private int port;
	private DefaultChannelGroup channelGroup;
	private ServerChannelFactory serverFactory;

	private ChannelGroup channels;
	private Timer timer;

	private ServerBootstrap bootstrap;
	private HazelcastInstance client;
	private CommandDispatcher dispatcher;

	private ServerCommandLineArguments jct;
	private ChannelPipelineFactory pipelineFactory;

	public Server(ServerCommandLineArguments jct) {
		this.host = jct.host;
		this.port = jct.port;
		this.jct = jct;
	}

	public void prepareHazelcastCluster() {

	}

	public void prepare() {
		channels = new DefaultChannelGroup();
		timer = new HashedWheelTimer();
		this.serverFactory = new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool());
		this.channelGroup = new DefaultChannelGroup(this + "-channelGroup");

		Config cfg = new Config();
		
		
		
		NetworkConfig network = cfg.getNetworkConfig();
		Join join = network.getJoin();
		join.getMulticastConfig().setEnabled(true);
		
		for (String ns: this.jct.hzcluster) {
			join.getTcpIpConfig().addMember(ns);
		} 
		
		join.getTcpIpConfig().setEnabled(true);				

		client = Hazelcast.newHazelcastInstance(cfg);

		/*
		 * We build up the dispatcher now ! Wish java had mixins
		 */
		List<Class<?>> klasses = new ArrayList<Class<?>>();
		klasses.add(ServerCommands.class);
		klasses.add(MapCommands.class);
		dispatcher = new CommandDispatcher(klasses);

		pipelineFactory = new ChannelPipelineFactory() {
			@Override
			public ChannelPipeline getPipeline() throws Exception {

				ServerHandler handler = new ServerHandler(channelGroup);
				handler.setClient(client);
				handler.setDispatcher(dispatcher);

				ChannelPipeline pipeline = Channels.pipeline();
				// pipeline.addLast("encoder", Encoder.getInstance());

				// pipeline.addLast("encoder", Command);
				pipeline.addLast("decoder", new RedisDecoder());
				pipeline.addLast("handler", handler);
				return pipeline;
			}
		};

	}

	public void start() {
		ServerBootstrap bootstrap = new ServerBootstrap(this.serverFactory);
		bootstrap.setOption("reuseAddress", true);
		bootstrap.setOption("child.tcpNoDelay", true);
		bootstrap.setOption("child.keepAlive", true);
		bootstrap.setPipelineFactory(pipelineFactory);

		Channel channel = bootstrap.bind(new InetSocketAddress(this.host,
				this.port));
		// if (!channel.isBound()) {
		// this.stop();
		// }

		this.channelGroup.add(channel);

	}

	public void stop() {
		if (this.channelGroup != null) {
			this.channelGroup.close();
		}
		if (this.serverFactory != null) {
			this.serverFactory.releaseExternalResources();
		}
	}

	public static void main(String[] args) {

        ServerCommandLineArguments jct_ = new ServerCommandLineArguments();    
        new JCommander(jct_, args);

        System.out.println("listening on: "+ jct_.host + ":" + jct_.port +"");

        final Server server = new Server(jct_);
        
        server.prepare();
        
        final long timeToWait = 1000 ;
         
        		
        while(true) {
        	try {
        		server.start();
        		break;
        	} catch (Exception e) {
        		server.port = server.port + 1;
        		try {        			
        			Thread.sleep(timeToWait);        		
        		} catch (InterruptedException i1) {
        			// pass 
        		}
        	}
        }        
        
        System.out.println("Mergen Server listening for commands..." + server.port);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                server.stop();
            }
        });
    }
}